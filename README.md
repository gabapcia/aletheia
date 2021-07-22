<p align="center"><a href="https://en.wikipedia.org/wiki/Aletheia">
<image src="https://upload.wikimedia.org/wikipedia/en/6/64/Van-gogh-shoes.jpg"></image>
</a></p>

<p align="center"><strong>Aletheia</strong> <em>- The state of not being hidden; the state of being evident</em></p>

---

Aletheia é um sistema de [ETL](https://en.wikipedia.org/wiki/Extract,_transform,_load) de dados de pessoas e empresas de fontes do governo, que visa unificar, simplificar e garantir o acesso a essas informações.

### Como funciona

Aletheia é um sistema escrito em Python 3, tendo o [Django](https://www.djangoproject.com) como framework web e [Celery](https://docs.celeryproject.org/en/stable/) como executor de tarefas.

Uma vez implantado, o sistema começará a fazer a síncronização com todas as fontes de dados automaticamente e sempre manterá os dados atualizados sem a necessidade de intervenção humana!

<p align="center">
<image src="./docs/images/schema.png"></image>
</p>

### Fontes de dados

Acesse a planilha com as fontes de dados [aqui](https://docs.google.com/spreadsheets/d/1BlCGMADJzvVbnHWsvQ7Y-4hxy_v5viECe30uCE4RrxM/edit?usp=sharing)

### Motivação

O acesso a dados cadastrais de empresas e pessoas é algo fundamental para diversos fins, como reportagens e sistemas de anti-fraude e score de crédito. Porém, o acesso à esses dados é complexo e trabalhoso visto que são diversos arquivos e muitas vezes não tem como consultar um único registro.
Com isso, veio a ideia de construir um sistema que fizesse todo esse trabalho de maneira automática e unificada para que o usuário precise acessar apenas um único sistema.

### Instalação

- Docker compose
    ```bash
    docker-compose up -d
    ```
