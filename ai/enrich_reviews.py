import os
import json
from mistralai.client import Mistral
import snowflake.connector
from openai import OpenAI
from google import genai
from dotenv import load_dotenv
from google.genai import types

load_dotenv()

# MODEL = "gpt-4o-mini"

SAMPLE_N = 50
TOPICS = ["food quality", "delivery", "pricing", "service", "packaging", "other"]
# client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
gemini_client = genai.Client(
    api_key=os.getenv("GEMINI_API_KEY")
)

GEMINI_MODEL = "gemini-2.5-flash"

mistral_client = Mistral(
    api_key=os.getenv("MISTRAL_API_KEY")
)

MISTRAL_MODEL = "mistral-small-latest"

grok_client = OpenAI(
    api_key=os.getenv("XAI_API_KEY"),
    base_url="https://api.x.ai/v1"
)

GROK_MODEL = "grok-4.5"


SYSTEM_PROMPT = f"""
You classify customer reviews for a food delivery app.

For the review you are given, return:
- sentiment_label: positive, negative, or neutral
- sentiment_score: a number between -1.0 and 1.0
- topic: one of {TOPICS}
- key_issue: a short phrase of 6 words or less that describes the main issue in the review, if any. If there is no issue, return null

Reply as JSON in this exact format:
{{
    "sentiment_label": "<sentiment_label>",
    "sentiment_score": <sentiment_score>,
    "topic": "<topic>",
    "key_issue": "<key_issue>"}}
"""

def get_connection():
    return snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
    )

def create_output_table(cursor):
    cursor.execute("CREATE SCHEMA IF NOT EXISTS ZOMATO.AI")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS ZOMATO.AI.REVIEW_ENRICHED (
            REVIEW_ID STRING,
            SENTIMENT_LABEL STRING,
            SENTIMENT_SCORE FLOAT,
            TOPIC STRING,
            KEY_ISSUE STRING,
            MODEL STRING,
            ENRICHED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
        )
    """)

def get_reviews_to_enrich(cursor):
    cursor.execute(f"""
        SELECT REVIEW_ID, COMMENT
        FROM ZOMATO.RAW.REVIEWS
        WHERE REVIEW_ID NOT IN (SELECT REVIEW_ID FROM ZOMATO.AI.REVIEW_ENRICHED)
        LIMIT {SAMPLE_N}
    """)
    return cursor.fetchall()

# def classify_review(comment):
#     response = client.chat.completions.create(
#         model=MODEL,
#         temperature=0,
#         response_format={"type": "json_object"},
#         messages=[
#             {"role": "system", "content": SYSTEM_PROMPT},
#             {"role": "user", "content": comment}
#         ]
#     )
#     answer = response.choices[0].message.content
#     return json.loads(answer)
# def classify_review(comment):
#     response = client.models.generate_content(
#         model=MODEL,
#         contents=comment,
#         config=types.GenerateContentConfig(
#             system_instruction=SYSTEM_PROMPT,
#             temperature=0,
#             response_mime_type="application/json"
#         )
#     )
#     answer = response.text
#     return json.loads(answer)


# def classify_with_gemini(comment):

#     response = gemini_client.models.generate_content(
#         model=GEMINI_MODEL,
#         contents=comment,
#         config=types.GenerateContentConfig(
#             system_instruction=SYSTEM_PROMPT,
#             temperature=0,
#             response_mime_type="application/json"
#         )
#     )

#     return json.loads(response.text)


# ============================================================
# MISTRAL
# ============================================================

# def classify_with_mistral(comment):

#     response = mistral_client.chat.complete(
#         model=MISTRAL_MODEL,
#         messages=[
#             {
#                 "role": "system",
#                 "content": SYSTEM_PROMPT
#             },
#             {
#                 "role": "user",
#                 "content": comment
#             }
#         ],
#         temperature=0
#     )

#     answer = response.choices[0].message.content
#     print(f"Mistral raw response: {answer}")
#     return json.loads(answer)
def classify_with_mistral(comment):

    response = mistral_client.chat.complete(
        model=MISTRAL_MODEL,
        messages=[
            {
                "role": "system",
                "content": SYSTEM_PROMPT
            },
            {
                "role": "user",
                "content": comment
            }
        ],
        temperature=0
    )

    answer = response.choices[0].message.content

    # print(f"Mistral raw response: {answer}")

    # Clean Markdown code fences if Mistral returns ```json ... ```
    answer = answer.strip()

    if answer.startswith("```json"):
        answer = answer[len("```json"):]

    elif answer.startswith("```"):
        answer = answer[len("```"):]

    if answer.endswith("```"):
        answer = answer[:-len("```")]

    answer = answer.strip()

    # Convert JSON string into Python dictionary
    return json.loads(answer)

# ============================================================
# GROK
# ============================================================

def classify_with_grok(comment):

    response = grok_client.responses.create(
        model=GROK_MODEL,
        input=[
            {
                "role": "system",
                "content": SYSTEM_PROMPT
            },
            {
                "role": "user",
                "content": comment
            }
        ]
    )

    answer = response.output_text
    # print(f"Grok raw response: {answer}")
    return json.loads(answer)



#fallback
def classify_review(comment):
    # 1. TRY GEMINI

    # try:
    #     print("Trying Gemini...")
    #     labels = classify_with_gemini(comment)
    #     print("Gemini succeeded.")
    #     return labels, GEMINI_MODEL

    # except Exception as e:
    #     print(f"Gemini failed: {e}")
    #     print("Falling back to Mistral...")

    # 2. TRY MISTRAL

    try:
        print("Trying Mistral...")
        labels = classify_with_mistral(comment)
        print("Mistral succeeded.")
        return labels, MISTRAL_MODEL

    except Exception as e:
        print(f"Mistral failed: {e}")
        print("Falling back to Grok...")


    # 3. TRY GROK
    try:
        print("Trying Grok...")
        labels = classify_with_grok(comment)
        print("Grok succeeded.")
        return labels, GROK_MODEL
    
    except Exception as e:
        print(f"Grok failed: {e}")

    # ALL PROVIDERS FAILED
    raise Exception(
        "All LLM providers failed: Gemini, Mistral, and Grok."
    )



def save_results(cursor, results):
    """Insert all the enriched rows into Snowflake in one go."""
    print(f"Saving {len(results)} enriched reviews to Snowflake...")
    cursor.executemany(
        """
        INSERT INTO ZOMATO.AI.REVIEW_ENRICHED
            (
                REVIEW_ID,
                SENTIMENT_LABEL,
                SENTIMENT_SCORE,
                TOPIC,
                KEY_ISSUE,
                MODEL
            )
        VALUES (%s, %s, %s, %s, %s, %s)
        """,
        results,
    )
 

def main():
    conn = get_connection()
    cursor = conn.cursor()
    create_output_table(cursor)
    reviews = get_reviews_to_enrich(cursor)

    if len(reviews) == 0:
        print("No new reviews to enrich.")
        return

    print(f"Enriching {len(reviews)} reviews...")

    results = []
    for review_id, comment in reviews:
        print(f"Classifying review {review_id}: {comment}")
        try:
            # labels = classify_review(comment)
            # print(f"Labels for review {review_id}: {labels}")
            # results.append((
            #     review_id,
            #     labels["sentiment_label"],
            #     labels["sentiment_score"],
            #     labels["topic"],
            #     labels["key_issue"],
            #     MODEL
            # ))
            labels, model_used = classify_review(comment)
            print(
                f"Labels for review {review_id}: {labels}"
            )
            print(
                f"Model used: {model_used}"
            )
            results.append(
                (
                    review_id,
                    labels["sentiment_label"],
                    labels["sentiment_score"],
                    labels["topic"],
                    labels["key_issue"],
                    model_used
                )
            )

        except Exception as e:
            print(f"Error occurred while classifying review {review_id}: {e}")

    save_results(cursor, results)
    print(f"Saved {len(results)} enriched reviews to Snowflake.")
    conn.commit()
    cursor.close()
    conn.close()

if __name__ == "__main__":
    main()