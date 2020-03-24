from tsv import TSV
from modifieur import Modifieur

from pymongo import MongoClient
 
client = MongoClient('localhost', 27017)


client.drop_database('imdb')

db = client.imdb
collection = db.titles



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
        
        
        
        line["_id"] = line['tconst']
        line["genres"] = Modifieur.spliter(line['genres'])
        line["isAdult"] = Modifieur.integer(line['isAdult'])
        line["startYear"] = Modifieur.integer(line["startYear"]) 
        line["endYear"] = Modifieur.integer(line["endYear"]) 
        line["runtimeMinutes"] = Modifieur.integer(line["runtimeMinutes"]) 
        
        liste.append(line)
    print(f"Tour n° {batch_number} sur {nombre_de_lignes} soit {round((batch_number/nombre_de_lignes)*100 , 2)}%")
    db.titles.insert_many(liste) 
      
    lines = tsv.read_sequential(10000)



