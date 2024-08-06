import os
from openai import AzureOpenAI
import pandas as pd
import json
import semantic_kernel as sk
import asyncio
from semantic_kernel.connectors.ai.open_ai import AzureChatCompletion
from semantic_kernel.connectors.search_engine.bing_connector import BingConnector
from semantic_kernel.functions.kernel_arguments import KernelArguments
from dotenv import load_dotenv
from semantic_kernel.core_plugins.web_search_engine_plugin import WebSearchEnginePlugin

load_dotenv()
# Load the Excel file
file_path = r"C:\Users\justin\gicsdata.xlsx"
xls = pd.ExcelFile(file_path)

# Load the data from each sheet
gics_1 = xls.parse('gics_1')
gics_2 = xls.parse('gics_2')
gics_3 = xls.parse('gics_3')
gics_4 = xls.parse('gics_4')

# Define the TreeNode class
class TreeNode:
    def __init__(self, code, name):
        self.code = int(code)
        self.name = name
        self.children = []

    def add_child(self, child_node):
        self.children.append(child_node)

    def to_dict(self):
        node_dictionary = {
            "code": self.code,
            "name": self.name,
        }
        return json.dumps(node_dictionary, indent=4)

# Create a dictionary to hold all nodes
node_dict = {}

# Create the root nodes from gics_1
for _, row in gics_1.iterrows():
    code = row['GICs 1']
    name = row['GICS 1 Description']
    node_dict[code] = TreeNode(code, name)

# Create level 2 nodes and attach them to level 1
for _, row in gics_2.iterrows():
    parent_code = row['GICs 1']
    code = row['GICS 2']
    name = row['GICS 2 Description']
    if pd.notna(code) and pd.notna(parent_code): # if parent code and code exist
        parent_node = node_dict[parent_code]
        node_dict[code] = TreeNode(code, name) #create curr node and store
        parent_node.add_child(node_dict[code]) #attach curr node to parent

# level 3
for _, row in gics_3.iterrows():
    parent_code = row['GICS 2']
    code = row['GICS 3']
    name = row['GICS 3 Description']
    if pd.notna(code) and pd.notna(parent_code):
        parent_node = node_dict[parent_code]
        node_dict[code] = TreeNode(code, name)
        parent_node.add_child(node_dict[code])

# level 4
for _, row in gics_4.iterrows():
    parent_code = row['GICS 3']
    code = row['GICS 4']
    name = row['GICS 4 Description']
    if pd.notna(code) and pd.notna(parent_code):
        parent_node = node_dict[parent_code]
        node_dict[code] = TreeNode(code, name)
        parent_node.add_child(node_dict[code])

root_nodes = [node for code, node in node_dict.items() if code in gics_1['GICs 1'].values]

def nodes_to_json_array(nodes): #array of nodes to array of json string
    json_array = []
    for node in nodes:
        json_dict = node.to_dict()
        json_str = json.dumps(json_dict, indent=4)
        json_array.append(json_str)
    return json_array

client = AzureOpenAI(
  api_key=os.getenv("AZURE_OPENAI_API_KEY"),  
    api_version="2024-02-01",
    azure_endpoint = os.getenv("AZURE_OPENAI_ENDPOINT")
    )

# deployment_name='gpt-35-turbo'    
deployment_name='gpt-4o'

df = pd.read_excel(r"C:\Users\justin\gicsdata.xlsx", sheet_name='Sheet1') #dataset RMB to change!!!!!!!!!!!!!!

def internal_name(df, idx):
    name = df.iloc[idx, 0]
    return {"internal_name": f"{name}"}

def prompter(df, idx):  #idx=row
    # Extract the first row, first col
    name = df.iloc[idx, 0]
    website = df.iloc[idx, 1]
    description = df.iloc[idx, 2]
    chat_input = f"Describe {name} and all the proucts they offer at {website}"
    kernel = sk.Kernel()
    kernel.add_service(
        AzureChatCompletion(
            env_file_path=".env",
            deployment_name = "gpt-35-turbo"
        ),
    )
    # bingsearch
    bing_search_api_key = os.getenv("BING_API_KEY")
    connector = BingConnector(bing_search_api_key)
    skill = kernel.add_plugin(WebSearchEnginePlugin(connector), plugin_name="WebSearch")
    skillfunc = skill["search"]
    try:
        async def main():
            result = await kernel.invoke(skillfunc, KernelArguments(query = chat_input, num_results = 4, offset = 0))
            print(result)
            return str(result)
        description = asyncio.run(main()) + description
    except Exception as e:
        print('search failed')
        print(e)
    finally:
        return "Analyze the provided company information: " + f"Company: {name} " + f"Description: {description}" 

def json_validator(input_data, lvl):
    print(input_data + "validate")
    data = json.loads(input_data)
    gics_code_key = f'gics_code_{lvl}'
    gics_level_key = f'gics_level_{lvl}'

    if gics_code_key in data and gics_level_key in data:
        if len(str(data[gics_code_key])) == 2 * lvl:
            return False
    return True

jsonres = []

def send_review_prompt(idx, lvl, table_str, prev_output, prompter):  #returns json string
    if lvl == 1:
        msg_prompt = "Tasks:\n" + \
            "1. Verify business description - Visit the company website to verify and update the company description. If not accessible, proceed with the provided description.\n" + \
            "2. Assign the primary industry at level 1 based on the company's core non-technological business activity.\n" + \
            "3. Given the following company description, using the GICS description provided, find the appropriate classification" + \
            "4. Fill the following JSON fields the most apppropriate classification:\n" + \
            '{\n' + \
            f'    "gics_level_{lvl}": "",\n' + \
            f'    "gics_code_{lvl}": ""\n' + \
            '}'
    else:
        msg_prompt = "Tasks:\n" + \
            f"1. Using the company description above, given that {prev_output}, using the GICS description provided, find the appropriate classification" + \
            "2. Fill the following JSON fields with the most appropriate classification:\n" + \
            '{\n' + \
            f'    "gics_level_{lvl}": "",\n' + \
            f'    "gics_code_{lvl}": ""\n' + \
            '}' 
    prompt1 = table_str + prompter + msg_prompt
    a1_msg = [{"role": "system", "content": "Assistant is an intelligent chatbot designed to help user generate GICS code based on information provided."},
                {"role": "user", "content": prompt1}] 
    
    response1 = client.chat.completions.create(model=deployment_name,
                                    messages=a1_msg,
                                    response_format={ "type": "json_object" })
    output1 = response1.choices[0].message.content  #return {level, code} json obj
    print(output1 + "...1")
    cond = json_validator(output1, lvl)
    while cond: #cond true when output wrong
        a1_msg.append({"role": "user", "content": f'Ensure that code has {lvl*2} digits'})
        response1 = client.chat.completions.create(model=deployment_name,
                                        messages=a1_msg,
                                        response_format={ "type": "json_object" })
        output1 = response1.choices[0].message.content  #return {level, code} json obj   
        cond = json_validator(output1, lvl)     
    json_str = output1

    #review portion
    msg_review = "Do you agree that the following description matches the GICS code." + json_str + \
            'If you agree, reply with the json field {"agree": "True"}\n' + \
            'If you disagree, respond with the following json field {"agree": "False", "reason": } and provide why you disagree in the "reason" field of the response\n'     
    prompt2 = table_str + prompter + msg_review
    a2_msg = [{"role": "system", "content": "Assistant is an intelligent chatbot designed to verify the GICS code provided."},
                {"role": "user", "content": prompt2}]
    response2 = client.chat.completions.create(model=deployment_name,
                                messages=a2_msg,
                                response_format={ "type": "json_object" })
    output2 = response2.choices[0].message.content #return {agree, reason}
    print(output2 + "...2")
    json_dict2 = json.loads(output2)
    flag = json_dict2["agree"] == "True"
    
    if flag:
        return json_str
    else:
        reason = json_dict2["reason"]
        # feed back to a1 by appending messages
        a1_msg.append({"role": "user", "content": "Tasks:\n" + \
                        f"1. Using the following GICS code description{table_str}, consider {output1} and {reason}, output the most appropriate GICS classification" + \
                        "2. Fill the following JSON fields:\n" + \
                                '{\n' + \
                                f'    "gics_level_{lvl}": "",\n' + \
                                f'    "gics_code_{lvl}": ""\n' + \
                                '}'
        })
        response3 = client.chat.completions.create(model=deployment_name,
                                        messages=a1_msg,
                                        response_format={ "type": "json_object" })
        output3 = response3.choices[0].message.content
        print(output3 + "...3")
        cond = json_validator(output3, lvl)
        while cond: #cond true when output wrong
            a1_msg.append({"role": "user", "content": f'Ensure that code has {lvl*2} digits'})
            response3 = client.chat.completions.create(model=deployment_name,
                                            messages=a1_msg,
                                            response_format={ "type": "json_object" })
            output3 = response3.choices[0].message.content  #return {level, code} json obj   
            cond = json_validator(output3, lvl)   
        # json_dict = json.loads(json_str3)
        return output3

def review_prompt(idx, lvl, table_str, curr_output, prompter):
    msg_prompt = "1. Verify business description - Visit the company website to verify and update the company description. If not accessible, proceed with the provided description.\n" + \
            f"2. Using the company description above, given that {curr_output}, using the GICS description provided, is the classification accurate?" + \
            'If you agree, reply with the json field {"agree": "True"}\n' + \
            'If you disagree, respond with the following json field {"agree": "False", "reason": } and provide why you disagree in the "reason" field of the response\n'
    prompt1 = table_str + prompter + msg_prompt
    a1_msg = [{"role": "system", "content": "Assistant is an intelligent chatbot designed to verify the GICS code provided."},
                {"role": "user", "content": prompt1}] 
    response1 = client.chat.completions.create(model=deployment_name,
                                    messages=a1_msg,
                                    response_format={ "type": "json_object" })
    output1 = response1.choices[0].message.content  #return {level, code} json obj
    print(output1 + "rev1")
    json_dict1 = json.loads(output1)
    flag = json_dict1["agree"] == "True"
    if flag:
        return curr_output
    else:
        a2_msg = [{"role": "system", "content": "Assistant is an intelligent chatbot designed to verify the GICS code provided."},
                    {"role": "user", "content": "Tasks:\n" + \
                                f"1. Considering the following GICS code description{table_str}, is {output1} or {curr_output} the more appropriate GICS classification" + \
                                "2. Fill the following JSON fields:\n" + \
                                        '{\n' + \
                                        f'    "gics_level_{lvl}": "",\n' + \
                                        f'    "gics_code_{lvl}": ""\n' + \
                                        '}'
                        }]
        response2 = client.chat.completions.create(model=deployment_name,
                                        messages=a2_msg,
                                        response_format={ "type": "json_object" })
        output2 = response2.choices[0].message.content
        print(output2 + "rev2")

        cond = json_validator(output2, lvl)
        while cond: #cond true when output wrong
            a2_msg.append({"role": "user", "content": f'Ensure that code has {lvl*2} digits'})
            response2 = client.chat.completions.create(model=deployment_name,
                                            messages=a2_msg,
                                            response_format={ "type": "json_object" })
            output2 = response2.choices[0].message.content  #return {level, code} json obj   
            cond = json_validator(output2, lvl)   
        # json_dict = json.loads(json_str3)
        return output2
def generate():
    for idx in range(len(df)): 
        output = "" # previous level output
        prev_dict = {}
        output_dict = internal_name(df, idx)
        prompt = prompter(df, idx)
        # level 1
        for i in range(1, 5, 1):
            try:   
                str_arr = []  
                if i == 1:
                    str_arr = nodes_to_json_array(root_nodes)
                else:
                    code = prev_dict[f"gics_code_{i-1}"]
                    node = node_dict[int(code)]
                    str_arr = nodes_to_json_array(node.children)
                table_str = ''.join(str_arr)
                output = send_review_prompt(idx, i, table_str, output, prompt)
                output = review_prompt(idx, i, table_str, output, prompt)
                # output = review_prompt(idx, i, table_str, output)
                prev_dict = json.loads(output)
                output_dict.update(prev_dict)
            except Exception as e:
                print(f"Exception caught at index {idx}, level {i}: {e}")
                continue

        jsonres.append(output_dict)
        print(output_dict)
        print("outputdict")
    print(jsonres)
    df_company = pd.DataFrame(jsonres)
    df_company.to_excel(r"C:\Users\justin\OneDrive\Desktop\nus\gicsoutput.xlsx", index=False, sheet_name='output') #output json data to excel

generate()