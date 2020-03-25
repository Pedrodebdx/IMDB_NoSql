from tsv import TSV
from modifieur import Modifieur

from pymongo import MongoClient

# connection à mongo 
client = MongoClient('localhost', 27017)
# suppression de la base avant nouvel import
client.drop_database('imdb')
#création de la base de données imdb
db = client.imdb
#création de la collection 'titles'
collection = db.titles




# j'instensie la classe TSV
tsv = TSV('./title.basics.tsv/data.tsv')
lines = []
batch_number = 1
line_number = 0
lines = tsv.read_sequential(10000)
print(lines)

while lines:
    batch_number += 1
    nombre_de_lignes = 3600
    liste = []
    for line in lines:
        line_number += 1
        
        
        # tconst devient l'identifiant
        line["_id"] = line['tconst']
        # genre passe par la class spliter pour etre splité
        line["genres"] = Modifieur.spliter(line['genres'])
        # adult passe dans la class int pour entre soit int soit nan
        line["isAdult"] = Modifieur.integer(line['isAdult'])
        line["startYear"] = Modifieur.integer(line["startYear"]) 
        line["endYear"] = Modifieur.integer(line["endYear"]) 
        line["runtimeMinutes"] = Modifieur.integer(line["runtimeMinutes"]) 
        
        liste.append(line)
    print(f"Tour n° {batch_number} sur {nombre_de_lignes} soit {round((batch_number/nombre_de_lignes)*100 , 2)}%")
    db.titles.insert_many(liste) 
      
    lines = tsv.read_sequential(10000)



