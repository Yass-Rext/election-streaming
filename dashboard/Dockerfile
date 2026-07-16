FROM python:3.11-slim

# Evite les fichiers .pyc
ENV PYTHONDONTWRITEBYTECODE=1

# Affiche les logs directement
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# Installation des dépendances
COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

# Copie du projet
COPY . .

# Port Streamlit
EXPOSE 8501

# Lancement
CMD ["streamlit", "run", "Home.py", "--server.address=0.0.0.0", "--server.port=8501"]