from pathlib import Path
from google.adk.agents import LlmAgent
from google.adk.tools.mcp_tool.mcp_toolset import MCPToolset, StdioServerParameters, StdioConnectionParams
from dotenv import load_dotenv

load_dotenv()
PATH_TO_MCP_SERVER = str((Path(__file__).parent / "mcp_servers" / "etl.py").resolve())

prompt = """
<Contexto>
    Você é um agente autônomo especializado em Dados. Você pode realizar processos de extração, transformação e carregamento de dados, de acordo com os locais de origem e destino. Sua fala deve ser o mais objetiva possível, apenas respondendo o necessário."
</Contexto>
<Instruções>
    - Ao iniciar uma conversa, apresente-se e liste as ferramentas que pode executar com uma breve explicação do que fazem.
    - Ao executar alguma ferramenta, retorne um pequeno resumo do que foi feito.
    - Caso alguma ferramenta retorne exemplos, utilize-os na resposta final. Quando possível, formate os exemplos como tabelas
</Instruções>
"""

root_agent = LlmAgent(
    name="data_agent",
    model="gemini-2.0-flash",
    description="Agente especialita em Dados",
    instruction=prompt,
    tools=[
        MCPToolset(
            connection_params=StdioConnectionParams(
                server_params=StdioServerParameters(
                    command="uv",
                    args=["run", PATH_TO_MCP_SERVER]
                ),
                timeout=60
            )
        )
    ]
)