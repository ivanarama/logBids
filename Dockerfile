FROM python:3.11-slim

WORKDIR /app
# Копируем .env
COPY .env /app/.env
# Копируем остальной код
COPY . /app

RUN pip install --no-cache-dir -r requirements.txt



#CMD ["sh"]
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]