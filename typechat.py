import os
from openai import AzureOpenAI
import pandas as pd
import re
import json
from dotenv import dotenv_values
import asyncio
from typechat import Failure, TypeChatJsonTranslator, TypeChatValidator, create_language_model
import schema as sch
from dotenv import load_dotenv

load_dotenv()
client = AzureOpenAI(
  api_key=os.getenv("AZURE_OPENAI_API_KEY"),  
    api_version="2024-02-01",
    azure_endpoint =os.getenv("AZURE_OPENAI_ENDPOINT")
    )
    
deployment_name='gpt-35-turbo'
env_vals = dotenv_values()
model = create_language_model(env_vals)
validator = TypeChatValidator(sch.GICSCode)
translator = TypeChatJsonTranslator(model, validator, sch.GICSCode)

msg_prompt = "Tasks:\n" + \
        "1. Verify business description - Visit the company website to verify and update the company description. If not accessible, proceed with the provided description.\n" + \
        "2. Identify primary business activity based on the company's description.\n" + \
        "3. Refer to the GICS Methodology document provided above to find the appropriate classification using the MSCI GICS structure.\n" + \
        "4. Match to MSCI GICs definitions to ensure alignment with the company’s operations.\n" + \
        "5. Assign the primary industry at level 1 based on the company's core non-technological business activity. Consider the influence of technology in subsequent levels.\n" + \
        "6. Identify the main country of operation, or the headquarters if multiple countries are involved.\n\n" + \
        "7. Give me the GICS code for all four levels."

msg_review = "Tasks:\n" + \
            "Refer to the GICS Methodology document provided above to find the appropriate classification using the MSCI GICS structure.\n" + \
            "1. If level 1 is Information Technology, check if the primary business activity based on the company's description.\n" + \
            "2. Ensure that all four GICS code are provided, generate any missing GICS code\n" + \
            "3. Check if the GICS code 1 to 4 matches each other and check if the number of digits is correct\n" + \
            "4. Check if the provided GICS code is correct. " + \
            "5. Double check if the GICS code is accurate at level 3 and 4, If there are any discrepancies or alternative GICS code, provide feedback. Fill this json field value below:\n" + \
            "6. Give me the GICS code for all four levels."

df = pd.read_excel(r"C:\Users\justin\OneDrive\Desktop\nus\gicsdata.xlsx", sheet_name='Sheet1') #dataset
table = pd.read_excel(r"C:\Users\justin\OneDrive\Desktop\nus\gicsdata.xlsx", sheet_name='gics') #gicstable

table_str = table.to_string(index=False) #gics table to string

def prompter(df, idx):  #idx=row
    # Extract the first row, first col
    name = df.iloc[idx, 0]
    website = df.iloc[idx, 1]
    description = df.iloc[idx, 2]
    return "Analyze the provided company information: " + f"Company: {name} " + f"Website: {website} " + f"Description: {description}" 

res1 = []
res2 = []
jsonres = []

async def request_handler(message: str):
    result = await translator.translate(message)
    if isinstance(result, Failure):
        return result.message
    else:
        result = result.value
        return result

async def send_review_prompt():
    for idx in range(len(df)):  #iterate through rows
        
        prompt = prompter(df,idx) + msg_prompt
        response1 = client.chat.completions.create(model=deployment_name,
                                      messages=[{"role": "system", "content": "Assistant is an intelligent chatbot designed to help user generate GICS code based on information provided."},
                                        {"role": "user", "content": table_str},
                                          {"role": "user", "content": prompt}])
        output = response1.choices[0].message.content
        res1.append(output)
        print(output)
        #review portion
        prompt = prompter(df,idx) + res1[idx] + msg_review
        response2 = client.chat.completions.create(model=deployment_name,
                                    messages=[{"role": "system", "content": "Assistant is an intelligent chatbot designed to verify the GICS code provided."},
                                                {"role": "user", "content": table_str},
                                                {"role": "user", "content": prompt}])
        output = response2.choices[0].message.content
        res2.append(output)
        print(output)
        # feed back to a1 by appending messages
        response3 = client.chat.completions.create(model=deployment_name,
                                      messages=[{"role": "system", "content": "Assistant is an intelligent chatbot designed to help user generate GICS code based on information provided."},
                                        {"role": "user", "content": table_str},
                                            {"role": "user", "content": prompt},
                                                {"role": "assistant", "content": res1[idx]},
                                                    {"role": "user", "content": res2[idx]},
                                                        {"role": "user", "content": "Do you agree with the above? Fill the following JSON fields with the accurate gics code:\n"
                                                            }])
        output = response3.choices[0].message.content
        print(output)
        # Extract JSON data from the provided string using typechat
        json_obj = await request_handler(output)
        jsonres.append(json_obj)

    df_company = pd.DataFrame(jsonres)
    df_company.to_excel(r"C:\Users\justin\OneDrive\Desktop\nus\gicsoutput.xlsx", index=False, sheet_name='output') #output json data to excel
    return res2



if __name__ == "__main__":
    asyncio.run(send_review_prompt())

# num of digits may be wrong