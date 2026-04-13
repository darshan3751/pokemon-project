import requests

def get_pokemon_data(pokemon_id):
    url = f"https://pokeapi.co/api/v2/pokemon/{pokemon_id}"
    data = requests.get(url).json()

    return {
        "id": data["id"],
        "name": data["name"],
        "height": data["height"],
        "weight": data["weight"],
        "base_experience": data["base_experience"],
        "type": data["types"][0]["type"]["name"]
    }