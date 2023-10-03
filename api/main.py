from fastapi import FastAPI
import pandas as pd

# Cargar los DataFrames aquí
df_steam_games = pd.read_csv('./data/steam_games_clean.csv')
df_genres = pd.read_csv('./data/genres_clean.csv')
df_user_reviews = pd.read_csv('./data/user_reviews_clean.csv')
df_users_items = pd.read_csv('./data/users_items_clean.csv')

# Crear una instancia de FastAPI
app = FastAPI()

# Saludar
@app.get('/')
def read_root():
    return {'Welcome to STEAM recommendation system'}

# Definir las funciones
def PlayTimeGenre(genero: str):
    # Filtrar juegos del género elegido
    df = df_genres[df_genres['genres'] == genero]

    if df.empty:
        return {'No se encuentran juegos del género elegido': None}
    
    # Combinar los DataFrames
    df = df.merge(df_steam_games, on='app_name', how='left')

    # Fusionar con df_users_items por 'app_name' y 'item_name'
    df = df.merge(df_users_items, left_on='app_name', right_on='item_name', how='inner')

    # Agrupar por año y calcular el tiempo de juego
    year_playtime = df.groupby(df['release_year'])['playtime_forever'].sum()

    # Devolver el año con más horas jugadas
    max_playtime_year = int(year_playtime.idxmax())

    return {'Año con más horas jugadas para el género ' + genero: max_playtime_year}

def UserForGenre(genero: str):
    # Filtrar juegos del género elegido
    df_genre = df_genres[df_genres['genres'] == genero]

    if df_genre.empty:
        return {'No se encuentran juegos del género elegido': None}

    # Combinar los DataFrames
    df_genre = df_genre.merge(df_steam_games, on='app_name', how='left')

    # Fusionar con df_users_items por 'app_name' y 'item_name'
    df_genre = df_genre.merge(df_users_items, left_on='app_name', right_on='item_name', how='inner')

    # Agrupar por usuario y calcular la suma de tiempo de juego
    user_playtime = df_genre.groupby('user_id')['playtime_forever'].sum()

    # Encontrar el usuario con más tiempo de juego
    max_playtime_user = user_playtime.idxmax()

    # Filtrar las filas del usuario con más tiempo de juego
    df_max_user = df_genre[df_genre['user_id'] == max_playtime_user]

    # Agrupar por año y calcular el tiempo de juego
    year_playtime = df_max_user.groupby(df_max_user['release_year'])['playtime_forever'].sum()

    # Filtrar años con 0 horas jugadas
    year_playtime = year_playtime[year_playtime > 0]

    # Crear la lista de acumulación de horas jugadas por año
    playtime_by_year = [{'Año': int(year), 'Horas': int(hours)} for year, hours in year_playtime.items()]

    return {"Usuario con más horas jugadas para Género " + genero: max_playtime_user, "Horas jugadas": playtime_by_year}

def UsersRecommend(year: int):

    # Filtrar las reseñas para el año dado
    df_filtered_reviews = df_user_reviews[df_user_reviews['posted_year'] == year]

    if df_filtered_reviews.empty:
        return {'No hay reseñas para el año dado': None}

    # Fusionar con df_users_items para obtener los nombres de los juegos
    df_filtered_reviews = df_filtered_reviews.merge(df_users_items, on='user_id', how='inner')
    df_filtered_reviews = df_filtered_reviews.merge(df_steam_games, left_on='item_name', right_on='app_name', how='inner')

    # Filtrar reseñas recomendadas y con sentimiento positivo o neutral
    df_filtered_reviews = df_filtered_reviews[(df_filtered_reviews['recommend'] == True) & 
                                              (df_filtered_reviews['sentiment_analysis'].isin([1, 2]))]

    # Contar la cantidad de reseñas por juego
    game_counts = df_filtered_reviews['app_name'].value_counts()

    # Obtener el top 3 de juegos más recomendados
    top_3_games = game_counts.head(3)

    # Crear el resultado en el formato deseado
    result = [{"Puesto " + str(i+1): game} for i, (game, _) in enumerate(top_3_games.items())]

    return result

def UsersNotRecommend(year: int):

    # Filtrar las reseñas para el año dado
    df_filtered_reviews = df_user_reviews[df_user_reviews['posted_year'] == year]

    if df_filtered_reviews.empty:
        return {'No hay reseñas para el año dado': None}

    # Fusionar con df_users_items para obtener los nombres de los juegos
    df_filtered_reviews = df_filtered_reviews.merge(df_users_items, on='user_id', how='inner')
    df_filtered_reviews = df_filtered_reviews.merge(df_steam_games, left_on='item_name', right_on='app_name', how='inner')

    # Filtrar reseñas recomendadas y con sentimiento negativo o neutral
    df_filtered_reviews = df_filtered_reviews[(df_filtered_reviews['recommend'] == False) & 
                                              (df_filtered_reviews['sentiment_analysis'].isin([0, 1]))]

    # Contar la cantidad de reseñas por juego
    game_counts = df_filtered_reviews['app_name'].value_counts()

    # Obtener el top 3 de juegos menos recomendados
    top_3_games = game_counts.head(3)

    # Crear el resultado en el formato deseado
    result = [{"Puesto " + str(i+1): game} for i, (game, _) in enumerate(top_3_games.items())]

    return result

def sentiment_analysis(year: int):

    # Filtrar reseñas del año dado
    df_filtered_reviews = df_user_reviews[df_user_reviews['posted_year'] == year]

    # Contar la cantidad de registros por sentimiento
    sentiment_counts = df_filtered_reviews['sentiment_analysis'].value_counts()

    return {
        'Negative': sentiment_counts.get(0, 0).item(),
        'Neutral': sentiment_counts.get(1, 0).item(),
        'Positive': sentiment_counts.get(2, 0).item()
    }

def user_recommendation(user_id: str):

    # Filtrar las reseñas del usuario
    user_reviews = df_user_reviews[df_user_reviews['user_id'] == user_id]

    # Obtener los juegos que el usuario ha recomendado
    juegos_recomendados = user_reviews[user_reviews['recommend'] == 1]['item_id']

    # Obtener usuarios similares
    usuarios_similares = df_user_reviews[df_user_reviews['item_id'].isin(juegos_recomendados)]['user_id'].unique()

    # Filtrar recomendaciones de usuarios similares
    recomendaciones = df_user_reviews[df_user_reviews['user_id'].isin(usuarios_similares)]
    recomendaciones = recomendaciones[recomendaciones['recommend'] == 1]

    # Contar la cantidad de recomendaciones por juego
    total_recomendaciones = recomendaciones['item_id'].value_counts()

    # Obtener los nombres de los juegos (sin repetir)
    juegos_recomendados = total_recomendaciones.head(5).index
    juegos_recomendados_nombres = df_steam_games[df_steam_games['id'].isin(juegos_recomendados)]['app_name'].unique()

    return juegos_recomendados_nombres.tolist()

# Registrar las rutas
@app.get('/playtimegenre/{genero}')
def playtime_genre(genero: str):
    return PlayTimeGenre(genero)

@app.get('/userforgenre/{genero}')
def userfor_genre(genero: str):
    return UserForGenre(genero)

@app.get('/usersrecommend/{year}')
def users_recommend(year: int):
    return UsersRecommend(year)

@app.get('/usersnotrecommend/{year}')
def users_not_recommend(year: int):
    return UsersNotRecommend(year)

@app.get('/sentimentanalysis/{year}')
def sentimentanalysis(year: int):
    try:
        res = sentiment_analysis(year)
        return res
    except Exception as e:
        return {
            'Message':'Something goes wrong',
            'Error' : str(e)
        }
    
@app.get('/userrecommendation/{user_id}')
def userrecommendation(user_id: str):
    return user_recommendation(user_id)