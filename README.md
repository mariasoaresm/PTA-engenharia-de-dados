<br />
<p align="center">
  <a href="https://github.com/CITi-UFPE/PTA-engenharia-de-dados">
    <img src="https://ci3.googleusercontent.com/mail-sig/AIorK4zWbC3U-G_vTTZE6rUQqJjzL8u7WNZjzhEaYi9z7slJn8vNhgnFVootxjm377GVCdPGY_F64WolHmGJ" alt="Logo" width="180px">
  </a>

  <h3 align="center">PTA Engenharia de Dados</h3>

  <p align="center">
    Este projeto foi criado em 2025.2 com a proposta de trazer a frente de engenharia de dados para o Processo de Treinamento de Área (PTA) do CITi. Ele foi desenvolvido com base em práticas modernas de engenharia de dados e tem como objetivo capacitar tecnicamente as pessoas aspirantes, alinhando-se às demandas atuais da empresa.
    <br />
    <br />
    <a href="https://github.com/CITi-UFPE/PTA-engenharia-de-dados"><strong>🔗 Explore a documentação »</strong></a>
    <br />
    <br />
    ·
    <a href="https://github.com/CITi-UFPE/PTA-engenharia-de-dados/issues">Reportar Bug</a>
    ·
    <a href="https://github.com/CITi-UFPE/PTA-engenharia-de-dados/issues">Solicitar Funcionalidade</a>
  </p>
</p>

<details open="open">
  <summary><h2 style="display: inline-block">Tabela de Conteúdo</h2></summary>
  <ol>
    <li><a href="#sobre-o-projeto">Sobre o Projeto</a></li>
    <li><a href="#instalação">Instalação</a></li>
    <li><a href="#execução">Execução</a>
      <ul>
        <li><a href="#usando-docker">Usando Docker</a></li>
        <li><a href="#localmente">Localmente</a></li>
      </ul>
    </li>
    <li><a href="#contato">Contato</a></li>
  </ol>
</details>

<br/>

## Sobre o Projeto

Este projeto foi desenvolvido para o Processo de Treinamento de Área (PTA) do CITi, com foco em engenharia de dados. Ele inclui uma API construída com **FastAPI**, utilizando boas práticas de desenvolvimento e uma estrutura modular para facilitar a manutenção e a escalabilidade. O objetivo principal do projeto é construir uma **pipeline completa** que possa ser acessada via uma API.

---

## 🛠️ Instalação

Para configurar o projeto em sua máquina, siga os passos abaixo:

1.  Certifique-se de que o **Python** (3.x) e o **Docker Desktop** estão instalados em sua máquina.

2.  **Clone o repositório:**
    ```bash
    git clone [https://github.com/CIT-UFPE/PTA-engenharia-de-dados.git](https://github.com/CIT-UFPE/PTA-engenharia-de-dados.git)
    ```

3.  **Entre na pasta do projeto:**
    ```bash
    cd PTA-engenharia-de-dados
    ```

---

## 🚀 Execução

Você pode rodar a aplicação usando Docker (recomendado) ou localmente.

### Usando Docker

Esta é a forma mais recomendada para garantir que todas as dependências estejam corretas.

1.  Certifique-se de que o **Docker Desktop** está em execução.

2.  **Suba os serviços** com o Docker Compose (isso irá construir a imagem e iniciar os containers):
    ```bash
    docker-compose up --build
    ```

3.  Acesse a aplicação em seu navegador no endereço:
    ```
    http://localhost:8000
    ```

4.  Para acessar a documentação interativa da API (Swagger UI), vá para:
    ```
    http://localhost:8000/docs
    ```

### Localmente

Se você preferir executar o projeto diretamente em sua máquina sem containers:

1.  Certifique-se de estar no **diretório principal** do projeto (`PTA-engenharia-de-dados`).

2.  **(Recomendado)** Crie e ative um ambiente virtual (venv).
    ```bash
    python -m venv .venv
    source .venv/bin/activate  # No Windows use: .venv\Scripts\activate
    ```

3.  **Instale as dependências** listadas no arquivo `requirements.txt`:
    ```bash
    pip install -r ./requirements.txt
    ```

4.  **Execute o projeto** usando Uvicorn:
    ```bash
    uvicorn app.main:app
    ```

5.  Acesse a aplicação em seu navegador no endereço:
    ```
    http://localhost:8000
    ```

6.  Para acessar a documentação interativa da API (Swagger UI), vá para:
    ```
    http://localhost:8000/docs
    ```

---

## Contato
<br/>

- [CITi UFPE](https://github.com/CITi-UFPE) - contato@citi.org.br
- [João Pedro Bezerra](https://github.com/jpbezera), Líder de Dados em 2025.2 - jpbmtl@cin.ufpe.br
