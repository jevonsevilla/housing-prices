from concurrent.futures import ThreadPoolExecutor

import pandas as pd
from langchain_core.prompts import ChatPromptTemplate
from langchain_ollama import OllamaLLM

template = (
    "You are tasked with extracting the specific building from the following content: {content}\n\n"
    "Please follow these strict formatting rules:\n\n"
    "1. **Extract Only Relevant Information:**\n"
    "   - Extract only the name of the building that directly matches the provided description: {parse_description}.\n"
    "   - Do not infer or assume details beyond what is explicitly mentioned.\n\n"
    "2. **Formatting Guidelines:**\n"
    "   - The extracted information must be formatted as a **proper noun** (e.g., 'Empire State Building').\n"
    "   - Do not include abbreviations, descriptions, or extra context.\n\n"
    "3. **Strict Output Rules:**\n"
    "   - No reasoning, comments, or explanations should be included.\n"
    "   - If no relevant building is found, return an empty string (`''`).\n"
    "   - If multiple matches exist, return **only the most likely match**.\n\n"
    "4. **Output Format (Strictly Enforced):**\n"
    "   - Your response should consist **only** of the extracted building name as a single string.\n"
    "   - Example valid output: `Burj Khalifa`\n"
    "   - Example invalid outputs:\n"
    "     - `'The building you are looking for is Burj Khalifa'` ❌\n"
    "     - `'Burj Khalifa is the tallest building in the world'` ❌\n"
    "     - `'I couldn't find a match'` ❌\n"
    "     - `'None'` ❌ (use `''` instead)\n"
    "     - `'Burj Khalifa'` ❌ (don't include quotations)\n"
)

template2 = (
    "Your task is to extract two pieces of information (property name, and transaction type) from the content below:\n\n"
    "INPUT CONTENT:\n"
    "{content}\n\n"
    "Refer to the following reference building description to guide your extraction:\n"
    "REFERENCE DESCRIPTION: {parse_description}\n\n"
    "Extract and return the following values, using **only** what is **explicitly stated** in the input content:\n\n"
    "1. **Building Name Extraction**\n"
    "   - Extract the name of the building only if it **directly and clearly matches** the reference description.\n"
    "   - **Do not infer** names based on context, style, or abbreviations.\n"
    "   - Format as a proper noun (e.g., 'Empire State Building').\n"
    "   - Do NOT include:\n"
    "     - Descriptive labels (e.g., 'condo', 'tower') unless they are part of the actual name.\n"
    "     - Property type, size, location, number of bedrooms, or price.\n"
    "     - Any abbreviation or short form.\n\n"
    "2. **Transaction Type Identification**\n"
    "   - Identify whether the content is related to a property for **sale**, **rent**, or **lease**.\n"
    "   - Valid values: `sale`, `rent`, `lease`, or an empty string (`''`).\n"
    "   - Return an empty string if:\n"
    "     - The transaction type is not explicitly stated.\n"
    "     - Multiple transaction types are mentioned (e.g., 'for rent or sale').\n"
    "     - Only vague terms like 'available' are used.\n"
    "   - Do NOT guess based on implied meaning or surrounding context.\n\n"
    "3. **Output Format (Strict)**\n"
    "   - Output must be a **single line** in this exact format: `<Building Name>|<Transaction Type>`\n"
    "   - If a value is missing, leave that part blank but preserve the pipe character.\n"
    "   - Examples:\n"
    "     - `Empire State Building|sale`\n"
    "     - `|lease`\n"
    "     - `One Central Park|`\n"
    "     - `|`\n"
    "   - **Do NOT** include:\n"
    "     - Quotes\n"
    "     - Extra spaces\n"
    "     - Explanations, markdown, or punctuation\n\n"
    "4. **Invalid Examples (Do Not Output Like This):**\n"
    "   - `'Empire State Building is for sale'` ❌\n"
    "   - `'I think it's for lease at Building B'` ❌\n"
    "   - `'Building C'` ❌ (missing transaction type — should be `Building C|`)\n"
    "   - `'lease'` ❌ (missing building — should be `|lease`)\n"
    "   - `'Building D' | 'lease'` ❌ (no quotes, no extra spacing)\n"
)


model = OllamaLLM(model="mistral")


def parse_with_ollama(content, parse_description):
    prompt = ChatPromptTemplate.from_template(template2)
    chain = prompt | model

    print(template2)
    response = chain.invoke(
        {"content": content, "parse_description": parse_description}
    )

    print(f"[Input]:  {content}")
    print(f"[Output]:{response}")
    return response


def batch_parse(df, text_column, parse_description, max_workers=5):
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        parsed = list(
            executor.map(
                lambda c: parse_with_ollama(c, parse_description), df[text_column]
            )
        )
    # Split the parsed_output into two columns: building and trans_type
    df[["building", "trans_type"]] = pd.DataFrame(
        [s.split("|", 1) if "|" in s else [s, ""] for s in parsed], index=df.index
    )
    return df


def main():
    # Example DataFrame
    df = pd.DataFrame(
        {
            "text": [
                "Shang Salcedo Place 1BR for sale + parking",
                "Ayala Triangle Tower for lease",
                "Rockwell Proscenium 2BR",
                "The Rise 3BR for rent",
                "Avingon for sale/rent",
                "Avingon for sale or rent",
            ]
        }
    )

    # Apply batch processing
    df = batch_parse(
        df,
        "text",
        "Extract only the building name and transaction type, know that it is in the Philippines. You need to be SURE of your response and strictly follow formats.",
    )
    print(df)

    return df


if __name__ == "__main__":
    test = main()
