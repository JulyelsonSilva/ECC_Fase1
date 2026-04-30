# Sistema ECC

Sistema web para gerenciamento do Encontro de Casais com Cristo (ECC), com suporte a múltiplas paróquias.

---

## 📌 Objetivo

Centralizar e organizar:

- Cadastro de encontristas (casais)
- Controle de equipes (encontreiros)
- Formação de círculos
- Gestão de palestras
- Visão consolidada de casais e equipes
- Controle de acesso por usuário

---

## 🏗️ Tecnologias

- Python 3
- Flask
- MySQL
- HTML + CSS
- Deploy: Railway / Render

---

## ⚙️ Funcionalidades

### 👥 Encontristas
- Cadastro completo de casais
- Datas de etapas (1ª, 2ª, 3ª)
- Telefones e endereço
- Observações

---

### 🛠️ Encontreiros (Equipes)
- Registro de participação em encontros
- Controle de equipes
- Status: Aberto / Concluído / Recusou / Desistiu
- Identificação de coordenadores

---

### 🔵 Círculos
- Criação automática a partir da equipe de círculos
- Controle de integrantes
- Coordenadores (original e atual)
- Cor do círculo

---

### 🎤 Palestras
- Vinculação com casal (via `id_casal`)
- Suporte a palestrante individual

---

### 🔎 Visão Casal
- Busca por nome completo ou apelido
- Filtro inteligente
- Exibição consolidada de histórico

---

### 🏢 Multi-paróquia
- Separação de dados por `paroquia_id`
- Seleção de paróquia via sessão

---

### 🔐 Controle de Acesso
- Login com senha criptografada
- Perfis de usuário
- Controle por paróquia

---

## 📂 Estrutura do Projeto
