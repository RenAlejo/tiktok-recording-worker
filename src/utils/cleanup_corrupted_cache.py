#!/usr/bin/env python3
"""
Script para limpiar entradas corruptas del cache de room_id
Elimina entradas con room_id vacío o None
"""

import sys
import os

# Agregar el directorio src al path para importar módulos
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.connection import db_manager
from database.models import RoomCache
from utils.logger_manager import logger
from sqlalchemy import or_

def cleanup_corrupted_cache():
    """Elimina entradas con room_id vacío o corrupto del cache"""
    try:
        with db_manager.get_session() as session:
            # Buscar entradas con room_id vacío, None, o solo espacios
            corrupted_entries = session.query(RoomCache).filter(
                or_(
                    RoomCache.room_id == "",
                    RoomCache.room_id == None,
                    RoomCache.room_id == " "
                )
            ).all()
            
            if not corrupted_entries:
                print("✅ No corrupted cache entries found.")
                return
            
            print(f"🔍 Found {len(corrupted_entries)} corrupted cache entries:")
            
            for entry in corrupted_entries:
                print(f"  - Username: {entry.username}, room_id: '{entry.room_id}', last_updated: {entry.last_updated}")
            
            # Confirmar eliminación
            response = input(f"\n🗑️  Delete {len(corrupted_entries)} corrupted entries? (y/N): ")
            
            if response.lower() in ['y', 'yes', 'sí', 'si']:
                # Eliminar entradas corruptas
                deleted_count = session.query(RoomCache).filter(
                    or_(
                        RoomCache.room_id == "",
                        RoomCache.room_id == None,
                        RoomCache.room_id == " "
                    )
                ).delete()
                
                session.commit()
                
                print(f"✅ Successfully deleted {deleted_count} corrupted cache entries.")
                logger.info(f"Cleaned up {deleted_count} corrupted cache entries")
            else:
                print("❌ Cleanup cancelled.")
                
    except Exception as e:
        print(f"❌ Error during cleanup: {e}")
        logger.error(f"Error during cache cleanup: {e}")

def show_cache_stats():
    """Muestra estadísticas del cache"""
    try:
        with db_manager.get_session() as session:
            total_entries = session.query(RoomCache).count()
            
            corrupted_entries = session.query(RoomCache).filter(
                or_(
                    RoomCache.room_id == "",
                    RoomCache.room_id == None,
                    RoomCache.room_id == " "
                )
            ).count()
            
            valid_entries = total_entries - corrupted_entries
            
            print(f"\n📊 Cache Statistics:")
            print(f"  Total entries: {total_entries}")
            print(f"  Valid entries: {valid_entries}")
            print(f"  Corrupted entries: {corrupted_entries}")
            
            if corrupted_entries > 0:
                print(f"  Corruption rate: {(corrupted_entries/total_entries)*100:.1f}%")
            
    except Exception as e:
        print(f"❌ Error getting cache stats: {e}")

if __name__ == "__main__":
    print("🧹 TikTok Live Recorder - Cache Cleanup Tool")
    print("=" * 50)
    
    # Mostrar estadísticas
    show_cache_stats()
    
    # Ejecutar limpieza
    cleanup_corrupted_cache()
    
    # Mostrar estadísticas finales
    print("\n📊 Final Statistics:")
    show_cache_stats()