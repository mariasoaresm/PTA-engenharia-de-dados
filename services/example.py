from app.schemas import ExampleData, ProcessedExampleData

def process_example_data(data: ExampleData) -> ProcessedExampleData:
    """
    Processa os dados de exemplo recebidos.

    Args:
        data (ExampleData): Dados brutos a serem processados.

    Returns:
        ProcessedExampleData: Dados processados.
    """
    # Exemplo simples de processamento: adicionar uma chave "processed" com valor True
    processed_data = ProcessedExampleData(
        id=data.id,
        name=data.name,
        value=data.value,
        processed=True
    )
    return processed_data