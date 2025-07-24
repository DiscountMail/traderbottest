import os
import asyncio
import logging
from datetime import datetime
import discord
from discord.ext import tasks
from dotenv import load_dotenv
from oandapyV20 import API
import oandapyV20.endpoints.pricing as pricing
import gspread
from google.oauth2.service_account import Credentials

# --- Configuration & Setup ---
load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)

# --- Environment Variable Loading & Validation ---
BOT_TOKEN = os.getenv("BOT_TOKEN")
try:
    CHANNEL_ID = int(os.getenv("CHANNEL_ID"))
    CHECK_INTERVAL_SECONDS = int(os.getenv("CHECK_INTERVAL_SECONDS", "60"))
except (TypeError, ValueError):
    logging.error("FATAL: CHANNEL_ID or CHECK_INTERVAL_SECONDS is not a valid integer.")
    exit()

OANDA_ACCOUNT_ID = os.getenv("OANDA_ACCOUNT_ID")
OANDA_ACCESS_TOKEN = os.getenv("OANDA_ACCESS_TOKEN")
OANDA_ENVIRONMENT = os.getenv("OANDA_ENVIRONMENT", "practice")
GOOGLE_SHEET_NAME = os.getenv("GOOGLE_SHEET_NAME")
SERVICE_ACCOUNT_FILE = os.getenv("SERVICE_ACCOUNT_FILE")
INSTRUMENTS_STR = os.getenv("INSTRUMENTS")
if not INSTRUMENTS_STR:
    logging.error("FATAL: INSTRUMENTS environment variable is not set.")
    exit()
INSTRUMENTS_LIST = [inst.strip() for inst in INSTRUMENTS_STR.split(',')]

if not all([BOT_TOKEN, CHANNEL_ID, OANDA_ACCOUNT_ID, OANDA_ACCESS_TOKEN, GOOGLE_SHEET_NAME, SERVICE_ACCOUNT_FILE]):
    logging.error("FATAL: A critical environment variable is missing. Check your .env file.")
    exit()

# --- API & Bot Initialization ---
oanda_api = API(access_token=OANDA_ACCESS_TOKEN, environment=OANDA_ENVIRONMENT)
intents = discord.Intents.default()
bot = discord.Client(intents=intents)

# --- Helper Functions ---
def auth_gspread():
    """Authenticates with Google using service account credentials."""
    try:
        scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
        creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=scopes)
        client = gspread.authorize(creds)
        logging.info("Successfully authenticated with Google Sheets API.")
        return client
    except FileNotFoundError:
        logging.error(f"Google credentials file not found at: {SERVICE_ACCOUNT_FILE}")
        return None
    except Exception as e:
        logging.error(f"Google Sheets Authentication Error: {e}")
        return None

def append_bulk_to_sheet(client, sheet_name, data_rows):
    """Appends a list of rows to the sheet in a single, efficient API call."""
    if not data_rows:
        return 0
    try:
        sheet = client.open(sheet_name).sheet1
        sheet.append_rows(data_rows, value_input_option='USER_ENTERED')
        logging.info(f"Successfully appended {len(data_rows)} rows to Google Sheet.")
        return len(data_rows)
    except Exception as e:
        logging.error(f"Google Sheet bulk append failed: {e}")
        return 0

# --- Core Bot Task Loop ---
@tasks.loop(seconds=CHECK_INTERVAL_SECONDS)
async def monitor_and_report():
    """The main task loop: Fetches from OANDA, writes to GSheets, and reports to Discord."""
    channel = bot.get_channel(CHANNEL_ID)
    if not channel:
        logging.warning("Cannot find Discord channel. Will retry next cycle.")
        return

    logging.info(f"Starting data fetch for {len(INSTRUMENTS_LIST)} instruments.")
    
    # 1. Fetch data from OANDA
    try:
        pricing_endpoint = pricing.PricingInfo(accountID=OANDA_ACCOUNT_ID, params={'instruments': INSTRUMENTS_STR})
        response = oanda_api.request(pricing_endpoint)
    except Exception as e:
        logging.error(f"OANDA API request failed: {e}")
        await channel.send(embed=discord.Embed(title="OANDA API Failure", description=f"Could not fetch data. Error: `{e}`", color=discord.Color.red()))
        return

    # 2. Process data for GSheets and Discord Table
    all_rows_for_gsheet = []
    table_rows_for_discord = []

    for price_data in response.get('prices', []):
        try:
            instrument = price_data['instrument']
            bid_price = price_data['bids'][0]['price']
            ask_price = price_data['asks'][0]['price']
            
            # Prepare row for Discord table (formatted string)
            # ljust() and rjust() pad the string to make columns align perfectly
            discord_row = f"{instrument.ljust(10)}| {bid_price.rjust(10)} | {ask_price.rjust(10)}"
            table_rows_for_discord.append(discord_row)
            
            # Prepare row for Google Sheets (list of values)
            dt_object = datetime.fromisoformat(price_data['time'].replace('Z', '+00:00'))
            timestamp_str = dt_object.strftime('%Y-%m-%d %H:%M:%S')
            gsheet_row = [instrument, timestamp_str, price_data['status'], bid_price, ask_price]
            all_rows_for_gsheet.append(gsheet_row)
            
        except (KeyError, IndexError, ValueError) as e:
            logging.warning(f"Could not parse price data for an instrument. Error: {e}")

    # 3. Write to Google Sheets
    rows_written = append_bulk_to_sheet(bot.gspread_client, GOOGLE_SHEET_NAME, all_rows_for_gsheet) if bot.gspread_client else 0

    # 4. Build the final Discord embed
    color = discord.Color.green() if rows_written > 0 and rows_written == len(INSTRUMENTS_LIST) else discord.Color.orange()
    embed = discord.Embed(
        title="OANDA Price Report",
        color=color,
        timestamp=datetime.utcnow()
    )

    # Build the table string for the embed field
    if table_rows_for_discord:
        header = f"{'Instrument'.ljust(10)}| {'BID'.rjust(11)} | {'ASK'.rjust(11)}"
        separator = "-" * len(header)
        table_content = "\n".join(table_rows_for_discord)
        # Combine everything into a single code block for perfect alignment
        full_table = f"```\n{header}\n{separator}\n{table_content}\n```"
        embed.add_field(name="Live Prices", value=full_table, inline=False)
    else:
        embed.description = "No price data was returned from the API."

    # Add summary fields at the bottom
    embed.add_field(name="Instruments Fetched", value=f"`{len(all_rows_for_gsheet)}`", inline=True)
    embed.add_field(name="Rows Written to GSheet", value=f"`{rows_written}`", inline=True)
    embed.set_footer(text="OANDA-GSheets Bot")
    
    # 5. Send the message to Discord
    try:
        await channel.send(embed=embed)
        logging.info("Cycle complete. Report sent to Discord.")
    except Exception as e:
        logging.error(f"Failed to send message to Discord: {e}")

# --- Bot Startup Event ---
@bot.event
async def on_ready():
    """Runs once when the bot successfully connects."""
    logging.info(f"Bot logged in as {bot.user.name}")
    bot.gspread_client = auth_gspread()
    monitor_and_report.start()

# --- Main Execution ---
if __name__ == "__main__":
    try:
        bot.run(BOT_TOKEN)
    except discord.errors.LoginFailure:
        logging.error("Login failed. The BOT_TOKEN is invalid.")
    except Exception as e:
        logging.error(f"A critical error occurred while starting the bot: {e}")
