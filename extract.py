import requests
import logging

def get_pokemon_data(pokemon_id):
    url = f"https://pokeapi.co/api/v2/pokemon/{pokemon_id}"
    
    try:
        response = requests.get(url)
        
        if response.status_code != 200:
            logging.error(f"Failed to fetch Pokemon {pokemon_id}")
            return None

        data = response.json()

        return {
            "id": data["id"],
            "name": data["name"],
            "height": data["height"],
            "weight": data["weight"],
            "base_experience": data["base_experience"],
            "type": data["types"][0]["type"]["name"]
        }

    except Exception as e:
        logging.error(f"Error fetching Pokemon {pokemon_id}: {e}")
        return None