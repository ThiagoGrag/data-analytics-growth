# Ingestão dos dados
Workspace -> Users -> thiagogdantis@gmail.com -> data-analytics-growth
Utilizei PySpark

## Pasta: bronze
Bases de order, consumer e restaurant subi utilizando notebooks e realizei o discovery incial nessa camada.
Não salvei dados nessa camada a não ser a base do teste_ab.



# Modelagem dos dados
## Pasta: silver -> silver_order
Fiz o join com a tabela customer e test_ab

Verifiquei se tinha campo nulo e excluí 8.505 dados sem customer_id
Retirei os campos que tinha valores zerado do order_total_amount

Criei as segmentações para a região, delivery_time e minimum_order_value.

Analisando os dados notei que temos poucos clientes inativos, cerca de 8 mil somente. Não considerei

## Pasta: silver -> analise_descritiva

Fiz avaliações de resumo e notei dados duplicados order_id. 
Retirei as duplicações.

## Pasta: gold -> model_order
Reduzi a base para balancear as variáveis.
Apliquei o modelo K-prototype.
Voltei com os tipos dos dados que o spark permite.
Transformei a base em pandas para spark novamente e salvei as tabelas com cluster
Criei os subsets para realizar as análises estatísticas dos testes ab.
Após rodar o modelo de clusterização, criei o subset para rodar o modelo estatístico na base com os cluster.
Restante do código não utilizei.

## Pasta: gold -> ab_test
Teste t para os dados gerais do teste ab. Análise simples para verificar se os dados brutos eram estatisticamente diferentes. Utilizei os valores já calculados para rodar mais rápido.
Rodei o modelo ANOVA para cada variável que decidi analisar.
Plataforma de origem
Faixa de Preço
Valor mínimo
Tempo entrega
Região
Cluster

# SQL Editor
Código básicos para verificações
# DASHBOARD
## order_analysis
Análise exploratória de todos os dados e entender comportamentos.
Divisão entre quantidade de pedidos e valores dos pedidos. Essa primeira visão não tinha excluído as duplicações ainda.

Entendimento de como o grupo target e control eram impactados.
Notei o desbalanceamento e depois ajustei esse ponto.

Decidi começar a analisar somente o ticket médio geral, ticket médio do cliente e a frequência de compra.

Decidi avaliar as 5 variáveis: Plataforma de origem; Faixa de Preço; Valor mínimo; Tempo entrega; Região


## ab_test_analysis
Com a base já balanceada e limpa, segui com a análise exploratória.
Avaliei todas as variáveis para entender se houve grandes mudanças.
Resolvi seguir analisado somente o ticket_medio e frequência

Verifiquei todas as variáveis pelo grupo do teste ab.

Após rodar o modelo do Cluster, avaliei os resultados também.

# Teste de Hipótese
1ª 
H0 = Não há diferença significativa entre o controle e o target no ticket médio por cliente
H1 = Há diferença significativa entre o controle e o target no ticket médio por cliente 

2ª 
H0 = Não há diferença significativa entre o controle e o target na frequência
H1 = Há diferença significativa entre o controle e o target na frequência 
