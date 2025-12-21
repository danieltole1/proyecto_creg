# src/vectordb.py
import chromadb
from sentence_transformers import SentenceTransformer
import logging

logger = logging.getLogger(__name__)

class VectorDB:
    """Gestor de ChromaDB para búsqueda vectorial de normas CREG"""
    
    def __init__(self, host="localhost", port=8000, collection_name="normativa_creg_v2"):
        """
        Conectar a ChromaDB y seleccionar colección
        
        Args:
            host: Host de ChromaDB
            port: Puerto de ChromaDB
            collection_name: Nombre de la colección
        """
        try:
            self.client = chromadb.HttpClient(host=host, port=port)
            self.collection = self.client.get_or_create_collection(
                name=collection_name,
                metadata={"hnsw:space": "cosine"}
            )
            self.embedding_model = SentenceTransformer('all-MiniLM-L6-v2')
            logger.info(f"✅ ChromaDB conectado. Colección: {collection_name}")
        except Exception as e:
            logger.error(f"❌ Error conectando a ChromaDB: {e}")
            raise
    
    def add_documents(self, documents, ids=None, metadatas=None):
        """
        Agregar documentos a ChromaDB
        
        Args:
            documents: Lista de textos
            ids: Lista de IDs únicos (auto-generados si None)
            metadatas: Lista de dicts con metadata
        
        Returns:
            True si éxito, False si falla
        """
        if not documents:
            logger.warning("No hay documentos para agregar")
            return False
        
        try:
            # Auto-generar IDs si no hay
            if ids is None:
                ids = [f"doc_{i}" for i in range(len(documents))]
            
            # Generar embeddings
            logger.info(f"Generando embeddings para {len(documents)} documentos...")
            embeddings = self.embedding_model.encode(documents)
            
            # Agregar a ChromaDB
            self.collection.upsert(
                ids=ids,
                documents=documents,
                embeddings=embeddings.tolist(),
                metadatas=metadatas if metadatas else [{}] * len(documents)
            )
            
            logger.info(f"✅ {len(documents)} documentos agregados a ChromaDB")
            return True
        
        except Exception as e:
            logger.error(f"❌ Error agregando documentos: {e}")
            return False
    
    def search(self, query, n_results=3):
        """
        Buscar documentos por similitud semántica
        
        Args:
            query: Texto de búsqueda
            n_results: Cantidad de resultados
        
        Returns:
            Dict con 'documents', 'distances', 'metadatas', 'ids' o None si falla
        """
        try:
            # Generar embedding del query
            query_embedding = self.embedding_model.encode(query)
            
            # Buscar en ChromaDB
            results = self.collection.query(
                query_embeddings=[query_embedding.tolist()],
                n_results=n_results
            )
            
            if results and results['documents']:
                logger.info(f"✅ Búsqueda exitosa: {len(results['documents'][0])} resultados")
                return results
            else:
                logger.warning("Sin resultados para la búsqueda")
                return None
        
        except Exception as e:
            logger.error(f"❌ Error en búsqueda: {e}")
            return None
    
    def get_collection_info(self):
        """Obtener info de la colección"""
        try:
            count = self.collection.count()
            logger.info(f"Colección '{self.collection.name}': {count} documentos")
            return {"name": self.collection.name, "count": count}
        except Exception as e:
            logger.error(f"❌ Error obteniendo info: {e}")
            return None
