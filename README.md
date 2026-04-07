# Lab1 Inteligencia de negocios


#How to run github.py

First you need to export github token to use the REST API. Export that first into your environment.


```
uv init . 
```

It must be made inside github cloned repo

Then to install dependencies:

```
uv sync 
```

Run script with: 

```
uv run github.py
```

GithubRepos csv source:
https://www.kaggle.com/datasets/donbarbos/github-repos
