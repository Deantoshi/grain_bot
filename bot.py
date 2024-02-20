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

QUERY_ID_LIST = ["3203507", "3294891", "3388794"]

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
    test = client.get_channel(CHANNEL_ID)
    await asyncio.sleep(8)
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
      await query_cooldown(BASE_COOLDOWN)
      run_all_networks(client)
      await query_cooldown(BASE_COOLDOWN)

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
    if filename == 'redemption_data.csv':
      # try to read user_borrowed column and if doesn't exist make it and set it to 0
      try:
          df = df[['Last_Redemption_Sent', 'Current_Redemption_Number', 'Network', 'current_time']]
      except:
          df = df[['Last_Redemption_Sent', 'Current_Redemption_Number', 'Network']]
          df['current_time'] = df['Last_Redemption_Sent']
          df['current_time'] = 0
    
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

  return df


#just returns the string we want our bot to post
def make_response_string(data, redemption_index):
  data = data.astype(str)

  response_string_2 = '>>> :rotating_light: [' + 'ERN Redemption #' + data['redemption_number'].iloc[
          redemption_index] + ' has occured.](' + data['tx_hash_minimal'].iloc[
              redemption_index] + ') :rotating_light:'
  response_string_2 += '\n \nRedemption Stats:\n \n'
  response_string_2 += '- Version Info: Ethos ' + data['configuration'].iloc[redemption_index][3:] + ' #' + data['config_redemption_number'].iloc[redemption_index]
  response_string_2 += '\n- Affected Trove Owner: [' + data[
      'trove_owner_super_minimal'].iloc[redemption_index] + '](' + data[
          'trove_owner_minimal'].iloc[redemption_index] + ') :frog:'
  response_string_2 += '\n- Date: ' + str(
      data['timestamp'].iloc[redemption_index])[:19] + ' UTC :hourglass:'
  response_string_2 += '\n- Collateral Redeemed: ' + data[
      'number_of_redeemed_tokens'].iloc[redemption_index] + ' $' + data[
          'redeemed_token'].iloc[
              redemption_index] + ' Tokens Valued at $' + data[
                  'redeemed_collateral_per_trove_owner_usd'].iloc[
                      redemption_index] + ' :dollar:'
  response_string_2 += '\n- Debt Repaid: ' + data['ern_debt_repaid'].iloc[
      redemption_index] + ' $' + 'ERN' + ' Valued at $' + data[
          'debt_repaid_per_trove_owner_usd'].iloc[
              redemption_index] + ' :currency_exchange:'
  response_string_2 += '\n- Redemption Net Profit / Loss: $' + data[
      'profit_or_loss_usd'].iloc[redemption_index] + ' :mirror_ball:'
  response_string_2 += '\n- Redemption Fee Percent: +' + data[
      'redemption_fee_percent'].iloc[redemption_index] + '% :palm_up_hand:'
  response_string_2 += '\n- Redemption Fee USD: $' + data[
      'redemption_fee_usd'].iloc[redemption_index] + ' :small_red_triangle:'
  response_string_2 += '\n- Total Redemption Profit / Loss to Date: $' + data[
      'total_aggregate_profit_or_loss'].iloc[redemption_index] + ' :moneybag:'
  response_string_2 += '\n- Total Collateral Redeemed to Date: $' + data[
      'total_aggregate_collateral_redeemed'].iloc[redemption_index] + ' :bank:'
  response_string_2 += '\n- Total Debt Repaid to Date: ' + data[
      'total_aggregate_ern_repaid'].iloc[
          redemption_index] + ' $ERN Valued at $' + data[
              'total_aggregate_debt_repaid'].iloc[
                  redemption_index] + ' :credit_card:'
  response_string_2 += '\n- Total Redemption Fees to Date: $' + data[
      'total_aggregate_redemption_fee'].iloc[redemption_index] + ' :gem:'

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

  messages = channel.history()

  # await query_cooldown(60)

  # Flatten the messages into a list
  # messages = [message async for message in messages]
  
  # gets messages in the order that they happened
  messages = [message async for message in channel.history(limit=None, oldest_first=True)]

  # message = messages[0]
  # print('attempting to delete message')
  # await message.delete()
  # print('message deleted')
  # await query_cooldown(25)
  # print(messages[-1].content)
  # # # Loop through the messages and edit them

  data = read_from_cloud_storage('test.csv')

  data = round_and_format_numbers(data)

  message_index = 0

  discord_index = 0

  while message_index < len(messages):
    message = messages[message_index]
    # print(message.content)
    if message.author == client.user:

      discord_message = make_response_string(data, discord_index)
      discord_index += 1
      
      
      await message.edit(content=discord_message)
      print(str(message_index) + '/' + str(len(messages)))

    await asyncio.sleep(7)
    message_index += 1
  # for message in messages:
  #   if message.author == client.user:
  #     await message.edit(content=".")
  #     print(".")


#Gets all of our channel's old messages
async def get_all_messages(channel):

  messages = channel.history()

  await query_cooldown(60)

  # Flatten the messages into a list
  messages = [message async for message in messages]

  if len(messages) < 1:
    get_all_messages(channel)
  return messages


#formats our df raw numbers
def round_and_format_numbers(data):

  #makes our percentages nicer to read
  data['redemption_fee_percent'] = data['redemption_fee_percent'] * 100

  data['number_of_redeemed_tokens'] = data['number_of_redeemed_tokens'].round(
      5)
  data['redemption_fee_tokens'] = data['redemption_fee_tokens'].round(5)
  #data['SP_ERN_Used_USD'] = data['SP_ERN_Used_USD'].round(5)

  data[[
      'redeemed_collateral_per_trove_owner_usd',
      'debt_repaid_per_trove_owner_usd', 'profit_or_loss_usd',
      'redemption_fee_percent', 'total_aggregate_profit_or_loss',
      'total_aggregate_collateral_redeemed', 'total_aggregate_debt_repaid',
      'total_aggregate_redemption_fee', 'ern_debt_repaid',
      'total_aggregate_ern_repaid', 'redemption_fee_usd'
  ]] = data[[
      'redeemed_collateral_per_trove_owner_usd',
      'debt_repaid_per_trove_owner_usd', 'profit_or_loss_usd',
      'redemption_fee_percent', 'total_aggregate_profit_or_loss',
      'total_aggregate_collateral_redeemed', 'total_aggregate_debt_repaid',
      'total_aggregate_redemption_fee', 'ern_debt_repaid',
      'total_aggregate_ern_repaid', 'redemption_fee_usd'
  ]].round(2)

  #takes the columns we want to re format
  number_columns = data[[
      'redeemed_collateral_per_trove_owner_usd',
      'debt_repaid_per_trove_owner_usd', 'profit_or_loss_usd',
      'number_of_redeemed_tokens', 'ern_debt_repaid', 'redemption_fee_usd',
      'redemption_fee_tokens', 'redemption_fee_percent',
      'total_aggregate_profit_or_loss', 'total_aggregate_collateral_redeemed',
      'total_aggregate_debt_repaid', 'total_aggregate_redemption_fee',
      'total_aggregate_ern_repaid'
  ]]

  number_columns = format_numbers(number_columns)

  data[[
      'redeemed_collateral_per_trove_owner_usd',
      'debt_repaid_per_trove_owner_usd', 'profit_or_loss_usd',
      'number_of_redeemed_tokens', 'ern_debt_repaid', 'redemption_fee_usd',
      'redemption_fee_tokens', 'redemption_fee_percent',
      'total_aggregate_profit_or_loss', 'total_aggregate_collateral_redeemed',
      'total_aggregate_debt_repaid', 'total_aggregate_redemption_fee',
      'total_aggregate_ern_repaid'
  ]] = number_columns[[
      'redeemed_collateral_per_trove_owner_usd',
      'debt_repaid_per_trove_owner_usd', 'profit_or_loss_usd',
      'number_of_redeemed_tokens', 'ern_debt_repaid', 'redemption_fee_usd',
      'redemption_fee_tokens', 'redemption_fee_percent',
      'total_aggregate_profit_or_loss', 'total_aggregate_collateral_redeemed',
      'total_aggregate_debt_repaid', 'total_aggregate_redemption_fee',
      'total_aggregate_ern_repaid'
  ]]

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
    data['config_redemption_number'] = [0]
    data['redemption_number'] = [0]
    data['timestamp'] = ['-1']
    # data['transaction'] = ['N/A']
    data['redeemed_token'] = ['N/A']
    data['redeemed_collateral_per_trove_owner_usd'] = [0]
    data['debt_repaid_per_trove_owner_usd'] = [0]
    data['profit_or_loss_usd'] = [0]
    data['number_of_redeemed_tokens'] = [0]
    data['ern_debt_repaid'] = [0]
    data['redemption_fee_usd'] = [0]
    data['redemption_fee_tokens'] = [0]
    data['redemption_fee_percent'] = [0]
    # data['trove_owner'] = ['N/A']
    data['tx_hash_minimal'] = ['N/A']
    data['trove_owner_minimal'] = ['N/A']

  return data

#adds cumulative stats irresepctive of our version
def add_cumulative_stats(data):
    #sorts our values for our cumulative sum of liquidation profits over days
  data = data.sort_values(by=['timestamp', 'redemption_number'], ascending=[True, True])
  data['total_aggregate_profit_or_loss'] = data['profit_or_loss_usd'].cumsum()
  data['total_aggregate_collateral_redeemed'] = data[
      'redeemed_collateral_per_trove_owner_usd'].cumsum()
  data['total_aggregate_debt_repaid'] = data[
      'debt_repaid_per_trove_owner_usd'].cumsum()
  data['total_aggregate_ern_repaid'] = data['ern_debt_repaid'].cumsum()
  data['total_aggregate_redemption_fee'] = data['redemption_fee_usd'].cumsum()
  data = data.reset_index(drop=True)
  
  #adds our cumulative redemption_number
  data['config_redemption_number'] = data['redemption_number']
  data['redemption_number'] = data.index
  data['redemption_number'] += 1
  data = index_fixer(data)
  data = data.sort_values(by=['timestamp', 'redemption_number'], ascending=[True, True])

  # data.to_csv('test.csv', index=False)
  df_write_to_cloud_storage(data, 'test.csv')
  return data

#Checks to see if we have a new liquidation to handle
# if so, goes through and sends out all of our new liquidation messages
async def new_message_handler(data, redemption_info_df, channel):

  last_redemption_sent = redemption_info_df['Last_Redemption_Sent'].iloc[0]
  current_redemption_number = redemption_info_df['Current_Redemption_Number'].iloc[0]

  #last_liquidation_sent = liquidation_info_df['Last_Liquidation_Sent'].iloc[0]
  #current_liquidation_number = liquidation_info_df['Current_Liquidation_Number'].iloc[0]

  if last_redemption_sent < current_redemption_number:
    #updates our testing csv
    # data.to_csv('test.csv', index=False)

    data = round_and_format_numbers(data)

    #makes our entire dataframe data types into a string
    #data = data.apply(str)

    await asyncio.sleep(8)

    #gets our message history
    while last_redemption_sent < current_redemption_number:

      print('last_redemption_sent: ' + str(last_redemption_sent))
      print('current_redemption_number: ' + str(current_redemption_number))

      message = make_response_string(data, last_redemption_sent)

      if last_redemption_sent < current_redemption_number:
        await send_discord_message(channel, message)

        last_redemption_sent += 1

        #updates our dataframe rows as needed
        redemption_info_df['Last_Redemption_Sent'] = last_redemption_sent
        redemption_info_df['Current_Redemption_Number']= current_redemption_number

        df_write_to_cloud_storage(redemption_info_df, 'redemption_data.csv')

        await asyncio.sleep(8)
  return


#will wait the amount of time needed before running everything again
#cooldown is in seconds
async def query_cooldown(cooldown):

  time_for_update = False

  df = read_from_cloud_storage('redemption_data.csv')
  last_update = int(df['current_time'].iloc[0])

  next_update = last_update + cooldown

  current_time = int(time.time())

  # if the current time is passed our cooldown threshold
  # we will set boolean to True and write to csv
  if current_time >= next_update:
    df['current_time'] = current_time
    # df.to_csv('cooldown.csv', index=False)
    df_write_to_cloud_storage(df, 'cooldown.csv')

    print('Ready to Update')
    time_for_update = True

  else:
    print('Current Time: ', current_time, ' Next Update: ', next_update)

  return time_for_update


#gets information about the last liquidation message sent out and the current liquidation number that has occured
def get_redemption_state_df(data):

  #holds the value of our last liquidation
  data_2 = read_from_cloud_storage('redemption_data.csv')

  #gives us the latest liquidation number
  current_redemption_number = data['redemption_number'].iloc[len(data) - 1]

  #updates our current_liquidation_number
  data_2['Current_Redemption_Number'] = current_redemption_number

  print('Last Redemption Sent, Current Redemption Number')
  print(
      data_2['Last_Redemption_Sent'].iloc[0],
      data_2['Current_Redemption_Number'].iloc[0])

  return data_2

#fixes indexing issues
def index_fixer(data):

  og_data = data

  config_column = data['configuration'].drop_duplicates()

  unique_configs = config_column.to_list()

  df_list = []

  for unique in unique_configs:

    data_copy = data.loc[data['configuration'] == unique]
    data_copy = data_copy.reset_index(drop=True)
    data_copy['config_redemption_number'] = data_copy.index
    data_copy['config_redemption_number'] += 1 
    df_list.append(data_copy)
  
  df = pd.concat(df_list)

  df = df.sort_values(by='timestamp', ascending=True)

  return df

#returns a string representing our configuration based off of the query_id
def get_config(query_id):
  if query_id == '3203507':
    configuration = 'OP V1'

  elif query_id == '3294891':
    configuration = 'OP V2'
  
  elif query_id == '3388794':
    configuration = 'OP V2.1'

  return configuration

#runs our discord bot
def run_discord_bot():

  intents = discord.Intents.default()

  #intents = discord.Intents.all()

  intents.messages = True

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
async def run_everything(client):
  
  channel = client.get_channel(CHANNEL_ID)

  await get_all_query_data(client, QUERY_ID_LIST)

  data = read_from_cloud_storage('test.csv')

  liquidation_info_df = get_redemption_state_df(data)

  #if we have a new liquidation
  await new_message_handler(data, liquidation_info_df, channel)

  time.sleep(8)
  return

# Will run all of our queries and put them in a csv file
# Remmoves duplicates from the CSV before writing to the file
async def get_all_query_data(client, query_id_list):

  for query_id in query_id_list:
    data = await query_extractor(client, query_id)
    data['configuration'] = get_config(query_id)

    csv_data = read_from_cloud_storage('test.csv')

    data = pd.concat([csv_data, data])

    data = data.drop_duplicates(subset=['trove_owner_super_minimal', 'tx_hash_minimal'], keep='last')

    data = add_cumulative_stats(data)
    
    # data.to_csv('test.csv', index=False)
    df_write_to_cloud_storage(data, 'test.csv')
  
  return


#runs all of our queries
async def run_all_networks(client):

  ready_to_update = False
  #makes sure that we are waiting for our cooldown before executing
  ready_to_update = await query_cooldown(BASE_COOLDOWN)

  if ready_to_update == True:
      await run_everything(client)
  
  print('Waiting for next update: ')
  await asyncio.sleep(BASE_COOLDOWN)
  ready_to_update = False
  await run_all_networks(client)
  return


#reads a csv_file instead of running our query
async def test_run_everything(client):

  channel = client.get_channel(CHANNEL_ID)

  data = read_from_cloud_storage('test.csv')

  liquidation_info_df = get_redemption_state_df(data)

  print(data)
  print(liquidation_info_df)
  #if we have a new liquidation
  await new_message_handler(data, liquidation_info_df, channel)

  #Deletes our old messages
  # await is_bot_message(client)
  # print('Messages Deleted')
  # await query_cooldown(100)

  # print('attempting to edit message')
  # await edit_messages(client)
  # print('edited')
  # await query_cooldown(25)

  return

def test_run():
  intents = discord.Intents.default()

  #intents = discord.Intents.all()

  intents.messages = True

  client = discord.Client(intents=intents)
  
  #prints when the bot is running and starts run_everything
  @client.event
  async def on_ready():
    await client.wait_until_ready()
    print(f'{client.user} is now running!')
    # await get_all_query_data(client, QUERY_ID_LIST)
    # await test_run_everything(client)
    # await edit_messages(client)
    await run_all_networks(client)
  
  client.run(DISCORD_TOKEN)

# test_run()
  
# data = pd.read_csv('test.csv')

# data = index_fixer(data)

# data = data.loc[data['configuration'] == 'OP V1']

# redemption_number_list = data['redemption_number'].to_list()

# not_in_list = []

# i = 1

# while i < len(redemption_number_list) - 1:
#   if i in redemption_number_list:
#     print('Found')
#   else:
#     not_in_list.append(i)
#   i += 1

# print(not_in_list)

# # can be used to reset some data
# df = pd.read_csv('redemptions.csv')
# df['current_time'] = 0
# df['Last_Redemption_Sent'] = 1761

# df_write_to_cloud_storage(df, 'redemption_data.csv')