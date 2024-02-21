from requests import get, post
import pandas as pd
import time
import discord
import os
import asyncio
import json
from functools import cache
import threading 
import queue
import time
import datetime
from concurrent.futures import ThreadPoolExecutor
# import gcs_updater
from google.cloud import storage
import google.cloud.storage
import os
import sys
import io
from io import BytesIO

# API_KEY = str(os.getenv("API_KEY"))
API_KEY = ''

HEADER = {"x-dune-api-key": API_KEY}

# DISCORD_TOKEN = str(os.getenv("DISCORD_TOKEN"))

DISCORD_TOKEN = ''

# CHANNEL_ID = 1148665283888816129  # granary liquidation-bot
CHANNEL_ID = 1146130743047757937  # deantoshi's granary-op
# CHANNEL_ID = 1141824480595955815 # deantoshi
# CHANNEL_ID = 1142956903073333269 # ethos-liquidations
# CHANNEL_ID = 1187462531497873408  # deantoshi redemption
# CHANNEL_ID = 1174873301110239293  # Ethos Redemption

# QUERY_ID = "3016371" # OP
# QUERY_ID = "3022471" # ARB

BASE_COOLDOWN = 86400

# QUERY_ID_LIST = ["3203507"]

QUERY_ID_LIST = ["3016371", "3022471", "3082379", "3063365", "3085277", "3090539"]

BASE_URL = "https://api.dune.com/api/v1/"

PATH = os.path.join(os.getcwd(), 'ethos-redemption-bot-d4d4be30b664.json')
# os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = PATH
STORAGE_CLIENT = storage.Client(PATH)
# print(STORAGE_CLIENT)

def make_api_url(module, action, ID):
  """
    We shall use this function to generate a URL to call the API.
    """

  url = BASE_URL + module + "/" + ID + "/" + action

  return url


def execute_query(query_id, engine="medium"):
  """
    Takes in the query ID and engine size.
    Specifying the engine size will change how quickly your query runs. 
    The default is "medium" which spends 10 credits, while "large" spends 20 credits.
    Calls the API to execute the query.
    Returns the execution ID of the instance which is executing the query.
    """

  url = make_api_url("query", "execute", query_id)
  params = {
      "performance": engine,
  }
  response = post(url, headers=HEADER, params=params)
  #print(response)
  execution_id = response.json()['execution_id']

  return execution_id


def get_query_status(execution_id):
  """
    Takes in an execution ID.
    Fetches the status of query execution using the API
    Returns the status response object
    """

  url = make_api_url("execution", "status", execution_id)
  response = get(url, headers=HEADER)

  return response


def get_query_results(execution_id):
  """
    Takes in an execution ID.
    Fetches the results returned from the query using the API
    Returns the results response object
    """

  url = make_api_url("execution", "results", execution_id)
  response = get(url, headers=HEADER)

  return response


def cancel_query_execution(execution_id):
  """
    Takes in an execution ID.
    Cancels the ongoing execution of the query.
    Returns the response object.
    """

  url = make_api_url("execution", "cancel", execution_id)
  response = get(url, headers=HEADER)

  return response


#loops through our query untill it is completed
async def get_populated_results(response, execution_id, client, query_id):
  state = response.json()['state']

  while state != 'QUERY_STATE_COMPLETED':
    print('Waiting on Query Completion: ' + state)
    await asyncio.sleep(15)
    #gets our updated response

    response = get_query_results(execution_id)
    state = response.json()['state']

    #adds some time if our query needs time to wait before executing
    if state == 'QUERY_STATE_PENDING':
      await asyncio.sleep(120)
      state = response.json(['state'])
    #if our query has an issue then we cancel the query. Sleep. and we run everything again
    if state != 'QUERY_STATE_COMPLETED' and state != 'QUERY_STATE_EXECUTING':
      cancel_query_execution(execution_id)
      print('Query cancelled and trying again later')
      run_all_networks(client)

    if state == 'QUERY_STATE_COMPLETED':
      print(state)
      break

  data = pd.DataFrame(response.json()['result']['rows'])

  return data

# # reads a csv from our GCP
# # filename = name of the file we want
def read_from_cloud_storage(filename):
    
    storage_client = storage.Client(PATH)
    bucket = storage_client.get_bucket('cooldowns')

    df = pd.read_csv(
    io.BytesIO(
                 bucket.blob(blob_name = filename).download_as_string() 
              ) ,
                 encoding='UTF-8',
                 sep=',')
    # 1st read setup
    if filename == 'liquidations.csv':
      # try to read user_borrowed column and if doesn't exist make it and set it to 0
      try:
          df = df[['Last_Liquidation_Sent','Current_Liquidation_Number','Network','current_time']]
      except:
        df = df
    
    # debt_repaid_per_trove_owner_usd,ern_debt_repaid,number_of_redeemed_tokens,profit_or_loss_usd,redeemed_collateral_per_trove_owner_usd,redeemed_token,redemption_fee_percent,redemption_fee_tokens,redemption_fee_usd,redemption_number,timestamp,transaction,trove_owner,trove_owner_minimal,trove_owner_super_minimal,tx_hash_minimal,total_aggregate_profit_or_loss,total_aggregate_collateral_redeemed,total_aggregate_debt_repaid,total_aggregate_ern_repaid,total_aggregate_redemption_fee,configuration,config_redemption_number

    # other read setup
    elif filename == 'test.csv':
      try:
          df = df[['debt_repaid_per_trove_owner_usd', 'ern_debt_repaid', 'number_of_redeemed_tokens', 'profit_or_loss_usd', 'redeemed_collateral_per_trove_owner_usd', 'redeemed_token', 'redemption_fee_percent', 'redemption_fee_tokens', 'redemption_fee_usd', 'redemption_number', 'timestamp', 'transaction', 'trove_owner', 'trove_owner_minimal', 'trove_owner_super_minimal', 'tx_hash_minimal', 'total_aggregate_profit_or_loss', 'total_aggregate_collateral_redeemed', 'total_aggregate_debt_repaid', 'total_aggregate_ern_repaid', 'total_aggregate_redemption_fee', 'configuration', 'config_redemption_number']]
      except:
          print('test csv failed')
    
    return df

def df_write_to_cloud_storage(df, filename):

    # storage_client = storage.Client(PATH)
    bucket = STORAGE_CLIENT.get_bucket('cooldowns')

    csv_string = df.to_csv(index=False)  # Omit index for cleaner output
    # print(csv_string)
    blob = bucket.blob(filename)
    blob.upload_from_string(csv_string)
    # print('')

    return


def format_numbers(df):
  for col in df:
    if df[col].dtype == float:
      df[col] = df[col].apply(lambda x: f"{x:,}")
  print(df.columns)
  return df


#just returns the string we want our bot to post
def make_response_string(data, liquidation_index, network):
  data[[
      'Liquidation_Number', 'Tx_Hash_Minimal', 'Timestamp',
      'collateral_amount', 'Token_Received', 'Value_Received', 'debt_amount',
      'Token_Paid', 'Value_Paid', 'Liquidation_Profit', 'liquidation_bonus',
      'Total_Aggregate_Profit'
  ]] = data[[
      'Liquidation_Number', 'Tx_Hash_Minimal', 'Timestamp',
      'collateral_amount', 'Token_Received', 'Value_Received', 'debt_amount',
      'Token_Paid', 'Value_Paid', 'Liquidation_Profit', 'liquidation_bonus',
      'Total_Aggregate_Profit'
  ]].astype(str)

  # print(len(data), data)
  # print(liquidation_index)
  # print(data['Liquidation_Number'][liquidation_index])
  response_string_2 = '>>> :rotating_light: [' + network + ' Liquidation #' + data[
      'Liquidation_Number'].iloc[liquidation_index] + ' has occured.](' + data[
          'Tx_Hash_Minimal'].iloc[liquidation_index] + ') :rotating_light:'
  response_string_2 += '\n \nLiquidation Stats:'
  response_string_2 += '\n \n- Date: ' + str(
      data['Timestamp'].iloc[liquidation_index])[:10] + ' UTC :hourglass:'
  response_string_2 += '\n- Collateral Liquidated: ' + data[
      'collateral_amount'].iloc[liquidation_index] + ' $' + data[
          'Token_Received'].iloc[
              liquidation_index] + ' Tokens Valued at $' + data[
                  'Value_Received'].iloc[liquidation_index] + ' :dollar:'
  response_string_2 += '\n- Debt Repaid: ' + data['debt_amount'].iloc[
      liquidation_index] + ' $' + data['Token_Paid'].iloc[
          liquidation_index] + ' Valued at $' + data['Value_Paid'].iloc[
              liquidation_index] + ' :currency_exchange:'
  response_string_2 += '\n- Liquidation Net Profit: +$' + data[
      'Liquidation_Profit'].iloc[liquidation_index] + ' :mirror_ball:'
  response_string_2 += '\n- Liquidation Bonus: +' + data[
      'liquidation_bonus'].iloc[liquidation_index] + '% :palm_up_hand:'
  response_string_2 += '\n- Aggregate Liquidation Profit to Date: +$' + data[
      'Total_Aggregate_Profit'].iloc[liquidation_index] + ' :moneybag:'
  #print('Short Form')
  return response_string_2


# Delete all messages sent by the bot
async def is_bot_message(client):
  channel = client.get_channel(CHANNEL_ID)
  #return message.author == client.user

  await channel.purge(limit=None, check=is_bot_message)
  print('tried to delete messages')


#edits all of our old messages
async def edit_messages(client):
  channel = client.get_channel(CHANNEL_ID)
  # Get all of the bot's old messages in the channel

  message_index = 0

  messages = channel.history()

  time.sleep(25)

  # Flatten the messages into a list
  messages = [message async for message in messages]

  print(messages[0])
  message = messages[0]
  print('attempting to delete message')
  await message.delete()
  print('message deleted')
  time.sleep(25)
  #print(messages[-1])
  # # Loop through the messages and edit them
  # while message_index < len(messages):
  #     if messages[message_index].author == client.user:
  #         await messages[message_index].edit(content="")
  #     i += 1
  for message in messages:
    if message.author == client.user:
      await message.edit(content=".")
      print(".")


#Gets all of our channel's old messages
async def get_all_messages(channel):

  messages = channel.history()

  time.sleep(60)

  # Flatten the messages into a list
  messages = [message async for message in messages]

  if len(messages) < 1:
    get_all_messages(channel)
  return messages


#formats our df raw numbers
def round_and_format_numbers(data):

  #makes our percentages nicer to read
  data['liquidation_bonus'] = data['liquidation_bonus'] * 100

  data['collateral_amount'] = data['collateral_amount'].round(5)
  data['debt_amount'] = data['debt_amount'].round(5)
  #data['SP_ERN_Used_USD'] = data['SP_ERN_Used_USD'].round(5)

  data[[
      'Liquidation_Profit', 'Value_Paid', 'Value_Received',
      'liquidation_bonus', 'Total_Aggregate_Profit'
  ]] = data[[
      'Liquidation_Profit', 'Value_Paid', 'Value_Received',
      'liquidation_bonus', 'Total_Aggregate_Profit'
  ]].round(2)

  #takes the columns we want to re format
  number_columns = data[[
      'Value_Received', 'Value_Paid', 'Liquidation_Profit',
      'liquidation_bonus', 'Total_Aggregate_Profit', 'collateral_amount',
      'debt_amount'
  ]]

  number_columns = format_numbers(number_columns)

  data['Value_Received'] = number_columns['Value_Received']
  data['Value_Paid'] = number_columns['Value_Paid']
  data['Liquidation_Profit'] = number_columns['Liquidation_Profit']
  data['liquidation_bonus'] = number_columns['liquidation_bonus']
  data['Total_Aggregate_Profit'] = number_columns['Total_Aggregate_Profit']
  data['collateral_amount'] = number_columns['collateral_amount']
  data['debt_amount'] = number_columns['debt_amount']

  #makes our entire dataframe data types into a string
  #data = data.apply(str)

  return data


#actually sends our discord message
async def send_discord_message(channel, message):

  await channel.send(message)

  return


#takes in a query_id and gets our query results in a dataframe
async def query_extractor(client, query_id):

  #gets our execution ID
  execution_id = execute_query(query_id, "medium")

  #makes our dataframe when our data is ready
  response = get_query_status(execution_id)

  response = get_query_results(execution_id)

  data = await get_populated_results(response, execution_id, client, query_id)

  if len(data) < 1:
    #
    data = pd.DataFrame()
    data['Liquidation_Number'] = [0]
    data['Timestamp'] = ['-1']
    data['Liquidation_Profit'] = [0]
    data['liquidation_bonus'] = [0]
    data['Value_Received'] = [0]
    data['Token_Received'] = ['N/A']
    data['Value_Paid'] = [0]
    data['Token_Paid'] = [0]
    data['Liquidator_Minimal'] = ['N/A']
    data['Tx_Hash_Minimal'] = ['N/A']
    data['liquidator_address'] = ['N/A']
    data['collateral_amount'] = [0]
    data['debt_amount'] = [0]
  #sorts our values for our cumulative sum of liquidation profits over days
  data = data.sort_values(by='Liquidation_Number', ascending=True)
  data['Total_Aggregate_Profit'] = data['Liquidation_Profit'].cumsum()
  data = data.reset_index(drop=True)

  return data


#Checks to see if we have a new liquidation to handle
# if so, goes through and sends out all of our new liquidation messages
async def new_message_handler(data, liquidation_info_df, channel, query_id):

  network = network_checker(query_id)

  last_liquidation_sent = liquidation_info_df.loc[
      liquidation_info_df['Network'] == network,
      'Last_Liquidation_Sent'].iloc[0]
  current_liquidation_number = liquidation_info_df.loc[
      liquidation_info_df['Network'] == network,
      'Current_Liquidation_Number'].iloc[0]

  #last_liquidation_sent = liquidation_info_df['Last_Liquidation_Sent'].iloc[0]
  #current_liquidation_number = liquidation_info_df['Current_Liquidation_Number'].iloc[0]

  if last_liquidation_sent < current_liquidation_number:
    #updates our testing csv
    # data.to_csv('test.csv', index=False)

    data = round_and_format_numbers(data)

    #makes our entire dataframe data types into a string
    #data = data.apply(str)

    await asyncio.sleep(15)

    #gets our message history
    while last_liquidation_sent < current_liquidation_number:

      print(network + ': ' + str(last_liquidation_sent) + '/' + str(current_liquidation_number))

      message = make_response_string(data, last_liquidation_sent, network)

      if last_liquidation_sent < current_liquidation_number:
        await send_discord_message(channel, message)

        last_liquidation_sent += 1

        #updates our dataframe rows as needed
        liquidation_info_df.loc[
            liquidation_info_df['Network'] == network,
            'Last_Liquidation_Sent'] = last_liquidation_sent
        liquidation_info_df.loc[
            liquidation_info_df['Network'] == network,
            'Current_Liquidation_Number'] = current_liquidation_number

        # liquidation_info_df.to_csv('liquidations.csv', index=False)
        df_write_to_cloud_storage(liquidation_info_df, 'liquidations.csv')

        await asyncio.sleep(10)
  return


#will wait the amount of time needed before running everything again
#cooldown is in seconds
async def query_cooldown(cooldown):

  time_for_update = False

  df = read_from_cloud_storage('liquidations.csv')
  last_update = int(df['current_time'].iloc[0])

  next_update = last_update + cooldown

  current_time = int(time.time())

  # if the current time is passed our cooldown threshold
  # we will set boolean to True and write to csv
  if current_time >= next_update:
    df['current_time'] = current_time
    # df.to_csv('cooldown.csv', index=False)
    df_write_to_cloud_storage(df, 'liquidations.csv')

    print('Ready to Update')
    time_for_update = True

  else:
    print('Current Time: ', current_time, ' Next Update: ', next_update)

  return time_for_update


#gets information about the last liquidation message sent out and the current liquidation number that has occured
def get_liquidation_state_df(data, query_id):

  network = network_checker(query_id)

  #holds the value of our last liquidation
  # data_2 = pd.read_csv('liquidations.csv')
  data_2 = read_from_cloud_storage('liquidations.csv')

  #gives us the latest liquidation number
  current_liquidation_number = data['Liquidation_Number'].iloc[len(data) - 1]

  #updates our current_liquidation_number
  data_2.loc[data_2['Network'] == network,
             'Current_Liquidation_Number'] = current_liquidation_number

  print('Last Liquidation Sent, Current Liquidation Number, Network')
  print(
      data_2.loc[data_2['Network'] == network,
                 'Last_Liquidation_Sent'].iloc[0],
      data_2.loc[data_2['Network'] == network,
                 'Current_Liquidation_Number'].iloc[0],
      data_2.loc[data_2['Network'] == network, 'Network'].iloc[0])

  return data_2


#checks what network a query_id is associated with and returns the corresponding string
def network_checker(query_id):
  network = ''

  if query_id == '3016371':
    network = 'OP'
  elif query_id == '3022471':
    network = 'ARB'
  elif query_id == '3082379':
    network = 'BASE'
  elif query_id == '3063365':
    network = 'ETH'
  elif query_id == '3085277':
    network = 'AVAX'
  elif query_id == '3090539':
    network = 'BNB'

  return network


#runs our discord bot
def run_discord_bot():

  intents = discord.Intents.default()

  #intents = discord.Intents.all()

  intents.messages = True

  # token = str(os.getenv("DISCORD_TOKEN"))
  client = discord.Client(intents=intents)

  #prints when the bot is running and starts run_everything
  @client.event
  async def on_ready():
    await client.wait_until_ready()
    print(f'{client.user} is now running!')
    # await run_everything(client, QUERY_ID)
    # await test_run_everything(client)
    await run_all_networks(client)

  client.run(DISCORD_TOKEN)

  return


#our runner function that:
# - finds the most up to date liquidation info from a query
# - finds what the last_liquidation_message we sent is and what the newest liquidation is
# - sends out a message if we have a new liquidation to report
# - starts a cooldown of 2 hours
# - looks for new query data and repeats
# will run once every 2 hours and only send a message if a new liquidation has occured
async def run_everything(client, query_id):

  channel = client.get_channel(CHANNEL_ID)

  data = await query_extractor(client, query_id)

  liquidation_info_df = get_liquidation_state_df(data, query_id)

  network = network_checker(query_id)
  #if we have a new liquidation
  await new_message_handler(data, liquidation_info_df, channel, query_id)

  #tells our script how long to wait before querying again
  print(network + ': Liquidations Complete')
  # query_cooldown(86400)
  print('Searching for Liquidations')

  #the last liquidation index our query found
  await run_everything(client, query_id)


# doesn't loop
async def run_everything_no_loop(client, query_id):

  channel = client.get_channel(CHANNEL_ID)

  data = await query_extractor(client, query_id)

  liquidation_info_df = get_liquidation_state_df(data, query_id)

  network = network_checker(query_id)
  #if we have a new liquidation
  await new_message_handler(data, liquidation_info_df, channel, query_id)

  #tells our script how long to wait before querying again
  print(network + ': Liquidations Complete')
  #query_cooldown(14400)
  print('Searching for Liquidations')

  #the last liquidation index our query found
  return


#runs all of our queries
async def run_all_networks(client):

  #makes sure we meet our cooldown requirements first
  ready_for_update = await query_cooldown(86400)

  i = 0
  if ready_for_update == True:
    for query_id in QUERY_ID_LIST:
      print('Queries Complete: ', i, ' / ', len(QUERY_ID_LIST))
      print(query_id)
      i += 1
      await run_everything_no_loop(client, query_id)
  
  await asyncio.sleep(15)
  await run_all_networks(client)
  return


#reads a csv_file instead of running our query
async def test_run_everything(client, query_id):

  channel = client.get_channel(CHANNEL_ID)

  data = pd.read_csv('test.csv')

  liquidation_info_df = get_liquidation_state_df(data, query_id)

  #if we have a new liquidation
  await new_message_handler(data, liquidation_info_df, channel, query_id)

  #Deletes our old messages
  # await is_bot_message(client)
  # print('Messages Deleted')
  # time.sleep(100)

  # print('attempting to edit message')
  # await edit_messages(client)
  # print('edited')
  # time.sleep(25)

  return

# df = pd.read_csv('liquidations.csv')
# df_write_to_cloud_storage(df, 'liquidations.csv')