import requests

url = "https://pokeapi.co/api/v2/pokemon/1"
data = requests.get(url).json()

print("Name:", data["name"])
print("Height:", data["height"])
print("Weight:", data["weight"])