import json
import os
from flask import Flask, jsonify, request, abort

app = Flask(__name__)

DATA_FILE = os.path.join(os.path.dirname(__file__), "data", "customers.json")


def load_customers():
    with open(DATA_FILE, "r") as f:
        return json.load(f)


@app.route("/api/health", methods=["GET"])
def health():
    return jsonify({"status": "ok", "service": "mock-server"})


@app.route("/api/customers", methods=["GET"])
def get_customers():
    customers = load_customers()

    try:
        page = int(request.args.get("page", 1))
        limit = int(request.args.get("limit", 10))
    except ValueError:
        return jsonify({"error": "page and limit must be integers"}), 400

    if page < 1 or limit < 1:
        return jsonify({"error": "page and limit must be positive integers"}), 400

    total = len(customers)
    start = (page - 1) * limit
    end = start + limit
    paginated = customers[start:end]

    return jsonify({
        "data": paginated,
        "total": total,
        "page": page,
        "limit": limit,
        "pages": (total + limit - 1) // limit
    })


@app.route("/api/customers/<string:customer_id>", methods=["GET"])
def get_customer(customer_id):
    customers = load_customers()
    customer = next((c for c in customers if c["customer_id"] == customer_id), None)
    if not customer:
        abort(404, description=f"Customer '{customer_id}' not found")
    return jsonify({"data": customer})


@app.errorhandler(404)
def not_found(e):
    return jsonify({"error": str(e.description)}), 404


@app.errorhandler(500)
def server_error(e):
    return jsonify({"error": "Internal server error"}), 500


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False)
