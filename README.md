# Data Agent

O **Data Agent** é um projeto focado em facilitar a manipulação, análise e integração de dados em diferentes fontes e formatos. Ele foi desenvolvido para ser modular, escalável e de fácil utilização.

## Funcionalidades

- Conexão com múltiplas fontes de dados
- Processamento e transformação de dados
- Exportação e integração com outros sistemas
- Suporte a automação de tarefas de dados

## Requerimentos

Para a instalação e execução do projeto é necessário possuir o `uv` instalado.
Siga o passo a passo descrito no link abaixo para instalação:
- [Documentação uv](https://docs.astral.sh/uv/getting-started/installation/)  

## Instalação

Clone o repositório:

```bash
git clone https://github.com/seu-usuario/data_agent.git
cd adk_agent
```

Inicie um ambiente virtual com uv:

```bash
uv venv
```

Ative o ambiente virtual:

Linux/mac
```bash
source .venv/bin/activate
```
Windows
```bash
.venv/Scripts/activate
```

Instale as dependencias:

```bash
uv sync
```

## Uso

Execute o servidor adk:

```bash
adk web
```

Abra a página web:

[http://localhost:8000](http://localhost:8000)

Selectione o agente de nome `data_agent` na interface.\


## Extras:
- Na pasta `data_model` está armazenado o modelo de dados planejado para a transformação final dos dados
- Na pasta `PowerBI` estão armazenados:
1. O arquivo `.pbix` com um dashboard de exemplo de utilização dos dados para analytics, e as configurações de ingestão do BigQuery e modelagem dos dados no PowerBI
2. 2 imagens .png demonstrando o processo de ingestão do BigQuery a partir do BigQuery e a modelagem de dados no PowerBI
3. 1 imagem .gif demonstrando o dashboard com alguns exemplos de views utilizadas para analytics dos dados.