<p align="center"><a href="https://en.wikipedia.org/wiki/Aletheia">
<image src="https://upload.wikimedia.org/wikipedia/en/6/64/Van-gogh-shoes.jpg"></image>
</a></p>

<p align="center"><strong>Aletheia</strong> <em>- The state of not being hidden; the state of being evident</em></p>

---

Aletheia é um sistema de [ETL](https://en.wikipedia.org/wiki/Extract,_transform,_load) de dados de pessoas e empresas de fontes do governo, que visa unificar, simplificar e garantir o acesso a essas informações.

### Motivação

O acesso a dados cadastrais de empresas e pessoas é algo fundamental para diversos fins, como reportagens e sistemas de anti-fraude e score de crédito. Porém, o acesso à esses dados é complexo e trabalhoso visto que são diversos arquivos e muitas vezes não tem como consultar um único registro.
Com isso, veio a ideia de construir um sistema que fizesse todo esse trabalho de maneira automátizada e unificada para que o usuário precise acessar apenas um único sistema.

### Como funciona

Aletheia é um sistema orquestrado pelo [Airflow](https://airflow.apache.org/), que sava os arquivos baixados no [MinIO](https://min.io/), os processa usando o [Spark](https://spark.apache.org/) e salva os dados estruturados no [ElasticSearch](https://www.elastic.co/) para pesquisa.

### Fontes de dados

Acesse a planilha com as fontes de dados [aqui](https://docs.google.com/spreadsheets/d/1BlCGMADJzvVbnHWsvQ7Y-4hxy_v5viECe30uCE4RrxM/edit?usp=sharing)

### Instalação

Atualmente o projeto ainda se encontra em desenvolvimento, então não há nenhuma forma definida de deploy. Porém, é possível rodar o projeto localmente usando o [docker-compose](https://docs.docker.com/compose/) e, se desejar, ainda é possível usar a função de [_Remote Container Development_ do Visual Studio Code (VSCode)](https://code.visualstudio.com/docs/remote/containers)

