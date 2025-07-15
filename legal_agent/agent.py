from dotenv import load_dotenv
from pathlib import Path

from google.adk.agents import LlmAgent
from google.adk.tools.mcp_tool.mcp_toolset import MCPToolset, StdioServerParameters, StdioConnectionParams

load_dotenv()
PATH_TO_MCP_SERVER = str((Path(__file__).parent / "mcp_server" / "legal.py").resolve())

prompt = """
<Contexto>
    Voce é um agente autônomo especializado em Análise de Dados. Você pode realizar processos de análise, visualização e relatórios de dados, analisando os dados de transações realizadas com cartão de crédito, a partir de tabela existente no BigQuery. Sua fala deve ser o mais objetiva possível, apenas respondendo o necessário.
</Contexto>
<Instruções>
    - Ao iniciar uma conversa, apresente-se e liste as ferramentas que pode executar com uma breve explicação do que fazem.
    - Sempre que possível, retorne a resposta estruturada como uma tabela.
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
</Instruções>"""

root_agent = LlmAgent(
    name="analytics_agent",
    model="gemini-2.0-flash",
    description="Agente especializado em análise de dados",
    instruction=prompt,
    tools=[
        MCPToolset(
            connection_params=StdioConnectionParams(
                server_params=StdioServerParameters(
                    command="uv",
                    args=["run", PATH_TO_MCP_SERVER]
                ),
                timeout=120
            )
        )
    ]
)