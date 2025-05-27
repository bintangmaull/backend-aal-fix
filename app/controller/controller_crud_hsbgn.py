import logging
from flask import request, jsonify
from app.service.service_crud_hsbgn import HSBGNService
from app.service.service_crud_bangunan import BangunanService  # tambahkan import

# Konfigurasi Logging
logging.basicConfig(level=logging.INFO)
loggerhsgbn = logging.getLogger(__name__)

class HSBGNController:
    @staticmethod
    def get_all():
        """Mengambil semua data HSBGN"""
        try:
            data = HSBGNService.get_all_hsbgn()
            return jsonify(data), 200
        except Exception as e:
            loggerhsgbn.error(f"Error saat mengambil semua data HSBGN: {e}")
            return jsonify({"error": "Terjadi kesalahan server"}), 500

    @staticmethod
    def get_by_id(hsbgn_id):
        """Mengambil satu HSBGN berdasarkan ID"""
        try:
            data = HSBGNService.get_hsbgn_by_id(hsbgn_id)
            if data:
                return jsonify(data), 200
            return jsonify({"error": "HSBGN tidak ditemukan"}), 404
        except Exception as e:
            loggerhsgbn.error(f"Error saat mengambil data HSBGN ID {hsbgn_id}: {e}")
            return jsonify({"error": "Terjadi kesalahan server"}), 500

    @staticmethod
    def get_by_kota(kota):
        """Mengambil HSBGN berdasarkan Kota"""
        try:
            data = HSBGNService.get_hsbgn_by_kota(kota)
            return jsonify(data), 200
        except Exception as e:
            loggerhsgbn.error(f"Error saat mengambil data HSBGN kota {kota}: {e}")
            return jsonify({"error": "Terjadi kesalahan server"}), 500

    @staticmethod
    def create():
        """Menambahkan HSBGN baru"""
        try:
            data = request.json or {}
            required_fields = ["kota", "provinsi", "hsbgn"]

            # Validasi apakah semua field wajib ada
            missing_fields = [f for f in required_fields if f not in data]
            if missing_fields:
                return jsonify({"error": f"Kolom wajib tidak lengkap: {', '.join(missing_fields)}"}), 400
            
            new_hsbgn = HSBGNService.create_hsbgn(data)
            return jsonify(new_hsbgn), 201
        except Exception as e:
            loggerhsgbn.error(f"Error saat menambahkan HSBGN: {e}")
            return jsonify({"error": "Terjadi kesalahan server"}), 500

    @staticmethod
    def update(hsbgn_id):
        """Mengedit data HSBGN dan langsung recalc bangunan di kota terkait."""
        try:
            data = request.json or {}
            updated = HSBGNService.update_hsbgn(hsbgn_id, data)
            if not updated:
                return jsonify({"error": "HSBGN tidak ditemukan"}), 404

            # trigger recalc untuk kota yang diperbarui
            kota = updated.get("kota")
            recalc_file = None
            if kota:
                recalc_file = BangunanService.recalc_by_kota(kota)

            return jsonify({
                "updated_hsbgn": updated,
                "recalc_file": recalc_file
            }), 200

        except Exception as e:
            loggerhsgbn.error(f"Error saat mengedit HSBGN ID {hsbgn_id}: {e}")
            return jsonify({"error": "Terjadi kesalahan server"}), 500
        
    @staticmethod
    def delete(hsbgn_id):
        """Menghapus data HSBGN"""
        try:
            if HSBGNService.delete_hsbgn(hsbgn_id):
                return jsonify({"message": "HSBGN berhasil dihapus"}), 204
            return jsonify({"error": "HSBGN tidak ditemukan"}), 404
        except Exception as e:
            loggerhsgbn.error(f"Error saat menghapus HSBGN ID {hsbgn_id}: {e}")
            return jsonify({"error": "Terjadi kesalahan server"}), 500

    @staticmethod
    def get_provinsi():
        """Mengambil daftar provinsi unik dari semua data HSBGN"""
        try:
            all_data = HSBGNService.get_all_hsbgn()
            provs = sorted({item['provinsi'] for item in all_data})
            return jsonify(provs), 200
        except Exception as e:
            loggerhsgbn.error(f"Error saat mengambil daftar provinsi: {e}")
            return jsonify({"error": "Terjadi kesalahan server"}), 500

    @staticmethod
    def get_kota_by_provinsi(provinsi):
        """Mengambil daftar kota unik berdasarkan provinsi"""
        try:
            all_data = HSBGNService.get_all_hsbgn()
            kotas = sorted({item['kota'] for item in all_data if item['provinsi'] == provinsi})
            return jsonify(kotas), 200
        except Exception as e:
            loggerhsgbn.error(f"Error saat mengambil kota untuk provinsi {provinsi}: {e}")
            return jsonify({"error": "Terjadi kesalahan server"}), 500
