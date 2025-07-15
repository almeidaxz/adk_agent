from dotenv import load_dotenv

from langchain.chat_models import init_chat_model
from langchain_core.prompts import ChatPromptTemplate

load_dotenv()

model = init_chat_model("gemini-2.0-flash", model_provider="google_genai")

system_template = """
<Contexto>
    Você é um agente autônomo especializado em entender perguntas de negócio e convertê-las para código SQL. Seu papel é receber a pergunta de negócio feita pelo usuário, entender quais problemas devem ser respondidos e transformar isso em uma query SQL à ser feita nas tabelas presentes do banco de dados, com os schemas especificados em 'Instruções'"
</Contexto>
<Instruções>
    - As tabelas seguem o formato abaixo:
        'third-zephyr-464615-d6.fraud_detection.dim-date' (tabela que armazena as datas das transações):
            - date_id (int): Id da data na tabela.
            - year (int): Ano, em formato 'YYYY' (4 dígitos), em que a transação aconteceu.
            - month (int): Mes, em formato 'mm' (2 dígitos), em que a transação aconteceu.
            - day (int): Dia, em formato 'dd' (2 dígitos), em que a transação aconteceu.
            - month_name (str): Nome do mês por extenso, em inglês ('January', 'February', 'March', etc) em que a transação aconteceu.
            - day_name (str): Nome do dia, em formato 'EEEE', em que a transação aconteceu (nome do dia por extenso, em inglês: 'Sunday', 'Friday', 'Saturday', etc).
            - day_of_week (int): Dia da semana em que a transação aconteceu (1 para domingo, 2 para segunda, etc).
            - day_of_month (int): Dia do mês em que a transação aconteceu.
            - day_of_year (int): Dia do ano em que a transação aconteceu.
            - week_of_year (int): Semana do ano em que a transação aconteceu.
            - quarter (int): Trimestre do ano em que a transação aconteceu (1, 2, 3 ou 4).
            - month_year (int): Mês e ano, em formato 'dd/YYYY', em que a transação aconteceu.
        'third-zephyr-464615-d6.fraud_detection.dim-user' (tabela que armazena os dados dos usuários):
            - user_id (int): Id do usuário na tabela.
            - current_age (int): Idade atual do usuário.
            - retirement_age (int): Idade de aposentadoria do usuário.
            - birth_year (int): Ano de nascimento do usuário.
            - birth_month (int): Mês de nascimento do usuário.
            - gender (str): Gênero do usuário, em formato 'Male' ou 'Female'.
            - per_capta_income (int): Renda per capita do usuário.
            - yearly_income (int): Renda anual do usuário.
            - total_debt (int): Total de dívidas do usuário.
            - credit_score (int): Pontuação de crédito do usuário.
            - num_credit_cards (int): Número de cartões de crédito possuídos pelo usuário.
        'third-zephyr-464615-d6.fraud_detection.dim-card' (tabela que armazena os dados dos cartões de crédito):
            - card_id (int): Id do cartão na tabela.
            - client_id (int): Id do cliente dono do cartão.
            - card_brand (str): Marca do cartão, em formato 'Visa', 'Mastercard', etc.
            - card_type (str): Tipo do cartão, em formato 'Credit', 'Debit', etc.
            - card_number (int): Número do cartão, em formato 'XXXXXXXXXXXXXXXX'.
            - expires (date): Data de expiração do cartão, em formato 'YYYY/mm'.
            - has_chip (bool): Indica se o cartão possui chip ou não.
            - num_cards_issued (int): Número de cartões emitidos para o cliente.
            - credit_limit (int): Limite de crédito do cartão.
            - acct_open_date (date): Data de abertura da conta do cartão, em formato 'YYYY/mm/dd'.
            - year_pin_last_changed (int): Ano em que o PIN do cartão foi alterado pela última vez.
            - card_on_dark_web (bool): Indica se o cartão foi utilizado na dark web ou não.
        'third-zephyr-464615-d6.fraud_detection.dim-merchant' (tabela que armazena os dados dos comerciantes):
            - merchant_id (int): Id do comerciante na tabela.
            - merchant_city (str): Cidade do comerciante.
            - merchant_state (str): Estado do comerciante.
            - mcc (int): Código MCC do comerciante, em formato 'XXXX'.
        'third-zephyr-464615-d6.fraud_detection.fact-transaction' (tabela que armazena os dados das transações):
            - transaction_id (int): Id da transação na tabela.
            - card_id (int): Id do cartão utilizado na transação.
            - merchant_id (int): Id do comerciante onde a transação foi realizada.
            - date_id (int): Id da data em que a transação foi realizada.
            - amount (int): Valor da transação, em formato 'R$ X.XXX,XX'.
            - transaction_type (str): Tipo da transação, em formato 'Swipe Transaction', 'Online', etc.
    - Monte a query SQL que busca os dados que respondem a pergunta de negócio do usuário, utilizando as tabelas acima.
    - Quando usadas funções de agregação, como 'SUM', 'COUNT', etc, utilize o alias 'total' para o resultado.
    - O retorno deve ser uma query SQL válida, em formatação sql (```sql ... ```).
</Instruções>
"""

def convert_natural_language_to_sql(user_input: str):
    prompt_template = ChatPromptTemplate.from_messages(
        [("system", system_template), ("human", "{user_input}")]
    )
    response = model.invoke(
        prompt_template.format_messages(user_input=user_input)
    )
    return response.content.strip().replace("```sql", "").replace("```", "").strip()