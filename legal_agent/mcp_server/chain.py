from dotenv import load_dotenv

from langchain.chat_models import init_chat_model
from langchain_core.prompts import ChatPromptTemplate

load_dotenv()

model = init_chat_model("gemini-2.0-flash", model_provider="google_genai")

system_template_sql_convertion = """
<Contexto>
    Você é um agente autônomo especializado em entender perguntas de negócio da área jurídica e convertê-las para código SQL. Seu papel é receber a pergunta feita pelo usuário, entender quais dados precisam ser buscados e transformar isso em uma query SQL à ser feita na tabela presente no banco de dados, com schema especificado em 'Instruções'"
</Contexto>
<Instruções>
    - As tabelas seguem o formato abaixo:
        'third-zephyr-464615-d6.legal.contracts' (tabela que armazena os dados da assinatura do contrato):
            - id (int): Id na tabela.
            - company_id (int): O id da empresa, FK para a tabela 'company.id'.
            - signed_date (date): Data em que o contrato foi assinado.
            - company_signer (str): Nome do responsável pela assinatura do lado da empresa cliente.
            - av_signer (str): Nome do responsável pela assinatura representando a nossa empresa.
            - contract_name (str): Nome do arquivo PDF do contrato.
        'third-zephyr-464615-d6.legal.company_info' (tabela que armazena os dados das empresas clientes):
            - id (int): Id na tabela.
            - company_name (str): Nome da empresesa cliente.
    - Monte a query SQL que busca os dados que respondem a pergunta de negócio do usuário, utilizando as tabelas acima.
    - Quando usadas funções de agregação, como 'SUM', 'COUNT', etc, utilize o alias 'total' para o resultado.
    - O retorno deve ser uma query SQL válida, em formatação sql (```sql ... ```).
</Instruções>
"""

system_template_document_extraction = """
<Contexto>
    Você é um agente autônomo especializado na extração de cláusulas específicas de um contrato. Seu papel é receber o texto completo de um contrato, e nele, buscar e retornar uma cláusula específica solicitada pelo usuário.
</Contexto>
<Instruções>
    - Busque pela cláusula específica, ou da que mais se aproxima do solicitado pelo usuário.
    - Retorne o texto completo, incluindo numeração e título.
</Instruções>
<Contrato>
{contract}
</Contrato>
"""

def convert_natural_language_to_sql(user_input: str) -> str:
    prompt_template = ChatPromptTemplate.from_messages(
        [("system", system_template_sql_convertion), ("human", "{user_input}")]
    )
    response = model.invoke(
        prompt_template.format_messages(user_input=user_input)
    )
    return response.content.strip().replace("```sql", "").replace("```", "").strip()

def extract_clause_from_document(document_text: str, data_needed: str) -> str:
    formated_prompt = system_template_document_extraction.format(contract=document_text)
    prompt_template = ChatPromptTemplate.from_messages(
        [("system", formated_prompt), ("human", "{user_input}")]
    )
    response = model.invoke(
        prompt_template.format_messages(user_input=data_needed)
    )
    return response.content