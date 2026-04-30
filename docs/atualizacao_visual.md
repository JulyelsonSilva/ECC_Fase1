# Plano de Atualização Visual do Sistema do ECC

## 1. Objetivo

Este documento define o plano de trabalho para a atualização visual do Sistema do ECC, com foco em modernizar a identidade visual do site, tornando-o mais profissional, institucional, acolhedor e visualmente coerente com a proposta do Encontro de Casais com Cristo.

A atualização terá impacto visual significativo, mas deverá preservar a estrutura funcional existente do sistema.

O objetivo é transformar a aparência do sistema sem alterar sua navegabilidade, seus fluxos de uso, suas rotas, seus formulários, sua lógica de permissões ou sua organização funcional.

---

## 2. Premissa principal

A mudança visual será radical na aparência, mas conservadora na estrutura.

Devem ser preservados:

- rotas Flask existentes;
- nomes de endpoints;
- permissões e condicionais Jinja;
- estrutura dos menus;
- links atuais;
- formulários atuais;
- nomes dos campos;
- IDs usados por JavaScript;
- lógica de busca;
- lógica de montagem;
- estrutura geral dos templates;
- organização principal das telas.

A nova identidade será aplicada como uma “nova pele” sobre o sistema atual.

---

## 3. Conceito visual aprovado

A linha visual escolhida será chamada de:

## Acolhimento ECC

O conceito combina:

- identidade católica;
- acolhimento pastoral;
- organização institucional;
- elegância visual;
- clareza para uso por voluntários;
- cores suaves;
- elementos gráficos fluidos;
- uso respeitoso do brasão do ECC nacional;
- cards e boxes modernos;
- imagens em marca-d’água;
- ícones visuais indicativos.

A interface deve transmitir confiança, simplicidade, organização e espírito de serviço.

---

## 4. Diretrizes visuais

### 4.1 Cores

A paleta será inspirada no brasão nacional do ECC, mas com tons amenizados para uso em interface digital.

Cores principais sugeridas:

- azul institucional: `#123D68`
- azul profundo: `#082B4D`
- azul suave: `#EAF1F8`
- dourado ECC: `#C99A2E`
- dourado claro: `#F4E4BB`
- bege de fundo: `#F7F1E8`
- fundo claro: `#FBF8F2`
- branco translúcido: `rgba(255, 255, 255, 0.86)`
- texto principal: `#1F2937`
- texto secundário: `#6B7280`
- borda suave: `#E6D8C0`

O vermelho presente no brasão deverá ser usado de forma pontual, preferencialmente para detalhes discretos, estados de alerta ou pequenos elementos decorativos.

---

### 4.2 Tipografia

A tipografia deve reforçar a diferença entre:

- títulos institucionais;
- textos funcionais;
- formulários;
- botões;
- descrições.

Sugestão conceitual:

- títulos com aparência mais institucional e elegante;
- textos e formulários com fonte simples e legível;
- hierarquia visual clara entre título, subtítulo e conteúdo.

Na primeira etapa, poderá ser mantida a base tipográfica atual do projeto, com melhoria em pesos, tamanhos, espaçamentos e contraste.

Posteriormente, pode-se avaliar o uso de fontes externas, desde que isso não prejudique performance ou estabilidade.

---

### 4.3 Brasão do ECC Nacional

O brasão nacional do ECC será integrado como elemento principal de identidade.

Uso recomendado:

- destaque na tela de login;
- uso na marca do topo do sistema;
- uso discreto em fundos ou marcas-d’água;
- uso moderado para evitar excesso visual.

Arquivo recomendado:

```text
static/img/brasao_ecc_nacional.png
