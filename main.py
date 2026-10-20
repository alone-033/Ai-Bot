import os
import asyncio
import json
import time
import traceback
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

import httpx
from telegram import Update
from telegram.constants import ChatAction
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    ContextTypes,
    filters,
)

TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
OPENROUTER_KEY = os.getenv("OPENROUTER_API_KEY")

# બેસ્ટ ફ્રી AI મોડેલ
MODEL = "deepseek/deepseek-r1:free"


SYSTEM_PROMPT = """
You are AN AI BOT, a capable and friendly AI assistant.
Answer accurately, naturally, and helpfully.
Reply in the same language as the user, including Gujarati, Hindi, and Hinglish.
Explain difficult topics clearly and use examples when useful.
For coding questions, give correct, practical code.
Never invent facts. If unsure, clearly say so.
Be concise for simple questions and detailed for complex ones.
Use readable formatting. Do not claim you searched the web unless you did.
"""


# --- 24/7 બોટને જગાડી રાખવા માટેનું નાનું સર્વર ---
class HealthCheckHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"Bot 24/7 chalu chhe!")

    def log_message(self, format, *args):
        pass


def start_dummy_server():
    port = int(os.environ.get("PORT", 8080))
    server = HTTPServer(("0.0.0.0", port), HealthCheckHandler)
    server.serve_forever()


# --- AI અને Telegram નું કામ ---
def get_headers():
    return {
        "Authorization": f"Bearer {OPENROUTER_KEY or ''}",
        "Content-Type": "application/json",
        "HTTP-Referer": "https://render.com",
        "X-Title": "AN AI BOT",
    }


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["history"] = []
    await update.message.reply_text(
        "👋 Hey! Main AN AI BOT hoon.\n\n"
        "Mujhse kuch bhi pooch sakte ho — coding, studies, "
        "ideas, writing ya general questions.\n\n"
        "💬 Apna sawaal bhejo, shuru karte hain!"
    )


async def reset(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["history"] = []
    await update.message.reply_text(
        "🔄 Chat context reset ho gaya! Naya sawaal poochho."
    )


async def typing_loop(context, chat_id):
    try:
        while True:
            await context.bot.send_chat_action(
                chat_id=chat_id,
                action=ChatAction.TYPING,
            )
            await asyncio.sleep(4)
    except asyncio.CancelledError:
        pass


async def stream_ai(messages):
    payload = {
        "model": MODEL,
        "messages": messages,
        "stream": True,
        "temperature": 0.3,
        "max_tokens": 1200,
    }

    timeout = httpx.Timeout(60.0, connect=15.0)

    async with httpx.AsyncClient(timeout=timeout) as client:
        async with client.stream(
            "POST",
            "https://openrouter.ai/api/v1/chat/completions",
            headers=get_headers(),
            json=payload,
        ) as response:
            if response.status_code == 429:
                raise RuntimeError("RATE_LIMIT")
            if response.status_code in (401, 403):
                raise RuntimeError("API_KEY")
            if response.status_code >= 400:
                error_body = await response.aread()
                print(f"OpenRouter Error ({response.status_code}): {error_body.decode(errors='ignore')}")
                raise RuntimeError(f"API_ERROR_{response.status_code}")

            async for line in response.aiter_lines():
                if not line.startswith("data:"):
                    continue

                data = line[5:].strip()
                if data == "[DONE]":
                    break

                try:
                    event = json.loads(data)
                    if "error" in event:
                        raise RuntimeError("STREAM_ERROR")

                    choices = event.get("choices", [])
                    if not choices:
                        continue

                    delta = choices[0].get("delta", {})
                    text = delta.get("content", "")

                    if isinstance(text, str) and text:
                        yield text

                except json.JSONDecodeError:
                    continue


async def handle_message(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):
    if not update.message or not update.message.text:
        return

    question = update.message.text.strip()
    if not question:
        return

    if not OPENROUTER_KEY:
        await update.message.reply_text(
            "⚙️ Setup error: OPENROUTER_API_KEY add karo."
        )
        return

    history = context.user_data.setdefault("history", [])
    history.append({"role": "user", "content": question})

    messages = [
        {"role": "system", "content": SYSTEM_PROMPT},
        *history[-12:],
    ]

    chat_id = update.effective_chat.id
    typing_task = asyncio.create_task(
        typing_loop(context, chat_id)
    )

    sent = None
    answer = ""
    last_edit = 0.0
    last_shown_text = ""

    try:
        sent = await update.message.reply_text("✍️ Soch raha hoon...")

        async for piece in stream_ai(messages):
            answer += piece
            visible = answer[:3900].strip()

            now = time.monotonic()
            if (
                visible
                and visible != last_shown_text
                and now - last_edit >= 1.2
            ):
                try:
                    sent = await sent.edit_text(visible)
                    last_edit = now
                    last_shown_text = visible
                except Exception:
                    pass

            if len(answer) >= 3900:
                answer = answer[:3900]
                break

        answer = answer.strip()

        if not answer:
            answer = "Is baar jawab nahi mil saka. Dobara try karo."

        if sent and answer != last_shown_text:
            try:
                await sent.edit_text(answer)
            except Exception as e:
                print(f"Final edit error: {e}")

        history.append({"role": "assistant", "content": answer})
        context.user_data["history"] = history[-12:]

    except RuntimeError as error:
        if str(error) == "RATE_LIMIT":
            message = "⏳ Free API ki limit hit hui hai. Thodi der baad try karo."
        elif str(error) == "API_KEY":
            message = "🔑 OpenRouter key invalid hai. Key check karo."
        else:
            message = f"⚠️ AI API error: {error}"

        if sent:
            await sent.edit_text(message)
        else:
            await update.message.reply_text(message)

        if history and history[-1].get("role") == "user":
            history.pop()

    except (httpx.TimeoutException, httpx.NetworkError):
        message = "🌐 Connection slow hai. Thodi der baad try karo."
        if sent:
            await sent.edit_text(message)
        else:
            await update.message.reply_text(message)

        if history and history[-1].get("role") == "user":
            history.pop()

    except Exception:
        traceback.print_exc()
        message = "⚠️ Kuch error aaya. Logs check karo."
        if sent:
            await sent.edit_text(message)
        else:
            await update.message.reply_text(message)

        if history and history[-1].get("role") == "user":
            history.pop()

    finally:
        typing_task.cancel()
        try:
            await typing_task
        except asyncio.CancelledError:
            pass


def main():
    if not TELEGRAM_TOKEN:
        raise RuntimeError("TELEGRAM_BOT_TOKEN missing hai.")

    # બેકગ્રાઉન્ડમાં 24/7 સર્વર ચાલુ કરો
    threading.Thread(target=start_dummy_server, daemon=True).start()

    app = Application.builder().token(TELEGRAM_TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("reset", reset))
    app.add_handler(
        MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message)
    )

    print("AN AI BOT is starting...")
    app.run_polling()


if __name__ == "__main__":
    main()
