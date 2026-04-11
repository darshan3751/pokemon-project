import requests

def get_pokemon_data(pokemon_id):
    url = f"https://pokeapi.co/api/v2/pokemon/{pokemon_id}"
    data = requests.get(url).json()

    return {
        "name": data["name"],
        "height": data["height"],
        "weight": data["weight"]
    }

pokemon = get_pokemon_data(1)

print("Pokemon Details")
print("----------------")
print("Name:", pokemon["name"])
print("Height:", pokemon["height"])
print("Weight:", pokemon["weight"])
