import json
import os
import requests
from openai import OpenAI

TELEGRAM_TOKEN = os.getenv('TELEGRAM_TOKEN')
OPENAI_API_KEY = os.getenv('OPENAI_API_KEY')

def lambda_handler(event, context):
    try:
        body = json.loads(event.get("body", "{}"))
        
        if "message" not in body or "text" not in body.get("message", {}):
            return {"statusCode": 200}
        
        chat_id = body["message"]["chat"]["id"]
        user_text = body["message"]["text"]
        
        client = OpenAI(api_key=OPENAI_API_KEY)
        resp = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "Eres experto en regulaciones CREG."},
                {"role": "user", "content": user_text}
            ],
            max_tokens=300
        )
        
        answer = resp.choices[0].message.content
        
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            json={"chat_id": chat_id, "text": answer}
        )
        
        return {"statusCode": 200}
        
    except Exception as e:
        print(f"Error: {e}")
        return {"statusCode": 500}
