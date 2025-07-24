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
from flask import Flask
from threading import Thread

# --- Configuration & Setup ---
load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)

# --- Web Server (to keep Render's Web Service alive) ---
app = Flask(__name__)

@app.route('/')
def health_check():
    """This route is called by Render to check if the app is alive."""
    return "Bot is alive and running!", 200

def run_web_server():
    """Runs the Flask web server."""
    # Render provides the PORT environment variable.
    port = int(os.environ.get("PORT", 8080))
    app.run(host='0.0.0.0', port=port)

# --- Environment Variable Loading (Your Bot's Code) ---
BOT_TOKEN = os.getenv("BOT_TOKEN")
# ... (The rest of your environment variable loading is exactly the same) ...
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
SERVICE_ACCOUNT_FILE = os.getenv("SERVICE_ACCOUNT_FILE", "wiseguys-c6650-710e87e96c88.json") # Default filename
INSTRUMENTS_STR = os.getenv("INSTRUMENTS")
if not INSTRUMENTS_STR: logging.error("FATAL: INSTRUMENTS not set."); exit()

# --- (Your bot's functions: auth_gspread, append_bulk_to_sheet, monitor_and_report, etc. go here) ---
# --- PASTE ALL OF YOUR BOT'S FUNCTIONS HERE, UNCHANGED ---
# --- For brevity, I am not re-pasting them, but you should. ---
def auth_gspread():
    try:
        scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
        creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=scopes)
        return gspread.authorize(creds)
    except Exception as e:
        logging.error(f"Google Sheets Auth Error: {e}")
        return None

def append_bulk_to_sheet(client, sheet_name, data_rows):
    if not data_rows: return 0
    try:
        sheet = client.open(sheet_name).sheet1
        sheet.append_rows(data_rows, value_input_option='USER_ENTERED')
        return len(data_rows)
    except Exception as e:
        logging.error(f"GSheet append failed: {e}")
        return 0

oanda_api = API(access_token=OANDA_ACCESS_TOKEN, environment=OANDA_ENVIRONMENT)
intents = discord.Intents.default()
bot = discord.Client(intents=intents)

@tasks.loop(seconds=CHECK_INTERVAL_SECONDS)
async def monitor_and_report():
    channel = bot.get_channel(CHANNEL_ID)
    if not channel: logging.warning("Cannot find Discord channel."); return

    try:
        pricing_endpoint = pricing.PricingInfo(accountID=OANDA_ACCOUNT_ID, params={'instruments': INSTRUMENTS_STR})
        response = oanda_api.request(pricing_endpoint)
    except Exception as e:
        await channel.send(embed=discord.Embed(title="OANDA API Failure", description=f"Error: `{e}`", color=discord.Color.red())); return

    all_rows_for_gsheet = []
    table_rows_for_discord = []

    for price_data in response.get('prices', []):
        try:
            instrument = price_data['instrument']
            bid_price = price_data['bids'][0]['price']
            ask_price = price_data['asks'][0]['price']
            discord_row = f"{instrument.ljust(10)}| {bid_price.rjust(10)} | {ask_price.rjust(10)}"
            table_rows_for_discord.append(discord_row)
            dt_object = datetime.fromisoformat(price_data['time'].replace('Z', '+00:00'))
            timestamp_str = dt_object.strftime('%Y-%m-%d %H:%M:%S')
            gsheet_row = [instrument, timestamp_str, price_data['status'], bid_price, ask_price]
            all_rows_for_gsheet.append(gsheet_row)
        except Exception as e:
            logging.warning(f"Could not parse price data. Error: {e}")

    rows_written = append_bulk_to_sheet(bot.gspread_client, GOOGLE_SHEET_NAME, all_rows_for_gsheet) if bot.gspread_client else 0
    
    color = discord.Color.green() if rows_written > 0 else discord.Color.orange()
    embed = discord.Embed(title="OANDA Price Report", color=color, timestamp=datetime.utcnow())
    if table_rows_for_discord:
        header = f"{'Instrument'.ljust(10)}| {'BID'.rjust(11)} | {'ASK'.rjust(11)}"
        separator = "-" * len(header)
        table_content = "\n".join(table_rows_for_discord)
        full_table = f"```\n{header}\n{separator}\n{table_content}\n```"
        embed.add_field(name="Live Prices", value=full_table, inline=False)
    embed.add_field(name="Instruments Fetched", value=f"`{len(all_rows_for_gsheet)}`", inline=True)
    embed.add_field(name="Rows Written to GSheet", value=f"`{rows_written}`", inline=True)
    await channel.send(embed=embed)


@bot.event
async def on_ready():
    logging.info(f"Bot logged in as {bot.user.name}")
    bot.gspread_client = auth_gspread()
    monitor_and_report.start()

# --- Main Execution Block ---
if __name__ == "__main__":
    # Start the web server in a separate thread
    web_thread = Thread(target=run_web_server)
    web_thread.daemon = True
    web_thread.start()
    
    # Run the Discord bot in the main thread
    bot.run(BOT_TOKEN)
