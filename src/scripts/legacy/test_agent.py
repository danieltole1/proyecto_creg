# test_agent.py
"""
Test del agente CREG (ChromaDB + Gemini)
"""

from src.agent import CREGAgent

def test_agent():
    print("\n" + "="*60)
    print("TEST: Agent CREG (ChromaDB + Gemini)")
    print("="*60)
    
    agent = CREGAgent()
    
    # Preguntas de prueba
    preguntas = [
        "¿Qué es la metodología para calcular tarifas?",
        "¿Cuáles son los estándares de calidad de servicio?",
        "¿Cómo se expanden las redes de distribución?"
    ]
    
    for pregunta in preguntas:
        print(f"\n📌 Pregunta: {pregunta}")
        resultado = agent.answer(pregunta)
        print(f"\n✅ Respuesta: {resultado['respuesta'][:300]}...\n")
        print(f"📚 Normas usadas: {len(resultado['normas_usadas'])}\n")

if __name__ == "__main__":
    test_agent()
