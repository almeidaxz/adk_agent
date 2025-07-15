import asyncio
import json
import os
import logging

import mcp.server.stdio

from pathlib import Path

from google.adk.tools.function_tool import FunctionTool
from google.adk.tools.mcp_tool.conversion_utils import adk_to_mcp_tool_type
from mcp import types as mcp_types
from mcp.server.lowlevel import NotificationOptions, Server
from mcp.server.models import InitializationOptions
from google.cloud import bigquery

from dotenv import load_dotenv

from chain import convert_natural_language_to_sql

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str((Path(__file__).parent / "credentials" / "credentials.json").resolve())
LOG_FILE_PATH = os.path.join(os.path.dirname(__file__), "mcp_server_activity.log")

load_dotenv()

logging.basicConfig(
  level=logging.DEBUG,
  format="%(asctime)s - %(levelname)s -[%(filename)s:%(lineno)d] - %(message)s",
  handlers=[
    logging.FileHandler(LOG_FILE_PATH, mode="a")
  ] 
)

app = Server("analytics_server")
client = bigquery.Client()

def convert_to_sql(user_input: str) -> str:
    """Gera uma query SQL que busca a resposta para o problema de negócio questionado pelo usuário.

    Args:
    user_input(str): Pergunta de negócio feita pelo usuário.

    Returns:
    str: Uma query SQL que busca os dados que respondem a pergunta de negócio do usuário.
    """
    sql = convert_natural_language_to_sql(user_input)
    logging.info(f"Convertido para SQL: {str(sql)}")
    return sql

def query_to_bq(query: str) -> str:
    """Executa a query SQL no BigQuery e retorna o resultado.

    Args:
    query(str): Query SQL a ser executada.

    Returns:
    str: Resultado da execução da query.
    """
    query_job = client.query(query)
    rows = list(query_job.result())
    logging.info(f"Resultado da busca no BQ: {str(rows)}")
    return str(rows)

def get_analysis(user_input: str) -> str:
    """Executa a cadeia de comandos para gerar uma query SQL e executá-la no BigQuery, retornando o resultado da busca.

    Args:
    user_input(str): Pergunta de negócio feita pelo usuário.

    Returns:
    str: Resultado da execução da query (uma tabela de dados em formato str).
    """
    query = convert_to_sql(user_input)
    result = query_to_bq(query)
    return result

ADK_ANALYTICS_TOOLS = {
    "get_analysis": FunctionTool(func=get_analysis)
}

@app.list_tools()
async def list_mcp_tools() -> list[mcp_types.Tool]:
    """MCP handler que lista as tools que este server expõe."""
    logging.info("MCP Server: Iniciou a listagem de tools.")
    mcp_tools_list = []
    for tool_name, adk_tool_instance in ADK_ANALYTICS_TOOLS.items():
        if not adk_tool_instance.name:
            adk_tool_instance.name = tool_name
        
        mcp_tools_schema = adk_to_mcp_tool_type(adk_tool_instance)
        mcp_tools_list.append(mcp_tools_schema)
    return mcp_tools_list

@app.call_tool()
async def call_mcp_tool(name: str, arguments: dict) -> list[mcp_types.TextContent]:
    """MCP handler para executar a chamada da tool solicitada pelo MCP client."""
    logging.info(f"MCP Server: Iniciou a chamada da tool {name} com os args: {arguments}")

    if name in ADK_ANALYTICS_TOOLS:
        adk_tool_instance = ADK_ANALYTICS_TOOLS[name]
        try:
            adk_tool_response = await adk_tool_instance.run_async(
               args=arguments,
               tool_context=None
            )
            logging.info(f"MCP Server: Tool {name} executada. Resposta: {adk_tool_response}")
            response_text = json.dumps(adk_tool_response, indent=2)
            return [mcp_types.TextContent(type="text", text=response_text)]
        except Exception as e:
            logging.error(f"MCP Server: Erro ao executar tool {name}. Resposta: {e}")
            payload = {
                "success": False,
                "message": f"Falha ao executar tool '{name}': {str(e)}"
            }
            error_text = json.dumps(payload)
            return [mcp_types.TextContent(type="text", text=error_text)]
    else:
        logging.error(f"MCP Server: Erro ao executar tool. Tool não implementada.")
        payload = {
        "success": False,
        "message": f"Tool não implementada neste servidor."
        }
        error_text = json.dumps(payload)
        return [mcp_types.TextContent(type="text", text=error_text)]

async def run_mcp_stdio_server():
    """Executa o MCP server, trabalhando no stdio"""
    async with mcp.server.stdio.stdio_server() as (read_stream, write_stream):
        await app.run(
            read_stream,
            write_stream,
            InitializationOptions(
                server_name=app.name,
                server_version="0.1.0",
                capabilities=app.get_capabilities(
                    notification_options=NotificationOptions(),
                    experimental_capabilities={}
                )
            )
        )


if __name__ == "__main__":
  logging.info(f"Iniciando servidor MCP.")
  try:
    asyncio.run(run_mcp_stdio_server())
  except Exception as e:
    logging.critical(f"Servidor MCP encontrou um erro não tratado: {e}", exc_info=True)
  finally:
    logging.info(f"Servidor MCP encerrando.")
    logging.info(f"\n\n")