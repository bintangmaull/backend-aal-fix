from flask import jsonify, current_app, send_file

from app.service.service_visualisasi_hazard import VisualisasiHazardService

def get_reference_curves(hazard_type):
    try:
        data = VisualisasiHazardService.get_curves(hazard_type)
        return jsonify(data), 200
    except ValueError as e:
        return jsonify({'error': str(e)}), 404
    except Exception as e:
        current_app.logger.error(f"Error fetching curves for {hazard_type}: {e}")
        return jsonify({'error': 'Internal server error'}), 500

def get_density_raster(hazard_type):
    try:
        path = VisualisasiHazardService.generate_density_geotiff(hazard_type)
        return send_file(path, as_attachment=False)
    except ValueError as e:
        return jsonify({'error': str(e)}), 404
    except Exception as e:
        current_app.logger.error(f"Error generating raster for {hazard_type}: {e}")
        return jsonify({'error': 'Internal server error'}), 500

def reload_all_rasters():
    try:
        VisualisasiHazardService.generate_all_density_geotiffs()
        return jsonify({'status': 'all rasters regenerated'}), 200
    except Exception as e:
        current_app.logger.error(f"Error reloading rasters: {e}")
        return jsonify({'error': 'Failed to reload rasters'}), 500
