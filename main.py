from flask import Flask, request, jsonify, abort, stream_with_context, send_file
from flask_sqlalchemy import SQLAlchemy
import sqlalchemy
from sqlalchemy.orm import aliased
from sqlalchemy import func, distinct
from datetime import datetime
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import json

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///elevator.db'
db = SQLAlchemy(app)


class EDemands(db.Model):
    """Base class that handles elevator demands data formatting
    """
    id = db.Column(db.Integer, primary_key=True)
    timestamp = db.Column(db.DateTime, default=datetime.utcnow)
    demand_floor = db.Column(db.Integer, nullable=False)

class EStatus(db.Model):
    """Base class that handles elevator status data formatting
    """
    id = db.Column(db.Integer, primary_key=True)
    demand_id = db.Column(db.Integer, nullable=False)
    timestamp = db.Column(db.DateTime, default=datetime.utcnow)
    current_floor = db.Column(db.Integer, nullable=False)
    vacant = db.Column(db.Boolean, nullable=False)

class DAnalytics(db.Model):
    """Base class that handles demands analytics data formatting
    """
    demand_id = db.Column(db.Integer, nullable=False, primary_key=True)
    demand_time = db.Column(db.DateTime, nullable=False)
    status_timediff = db.Column(db.Integer, nullable=False)
    floordiff = db.Column(db.Integer, nullable=False)
    resting_floor = db.Column(db.Integer, nullable=False)
    end_floor = db.Column(db.Integer, nullable=False)

with app.app_context():
    db.create_all()

@app.route('/demand', methods=['POST'])
def create_demand():
    data = request.get_json()
    if type(data['demand_floor']) is not int or data['demand_floor'] < 0:
        return jsonify({'FAILED': 'floor is either negative or a non integer'}), 400
    new_demand = EDemands(demand_floor=data['demand_floor'])
    db.session.add(new_demand)
    db.session.commit()
    return jsonify({'SUCESS': 'Demand created'}), 200


@app.route('/state', methods=['POST'])
def create_status():
    data = request.get_json()
    if type(data['current_floor']) is not int or data['current_floor'] < 0:
        return jsonify({'FAILED': 'current_floor is either negative or a non integer'}), 400

    if type(data['vacant']) is not bool:
        return jsonify({'FAILED': 'vacant is not boolean type'}), 400

    new_status = EStatus(current_floor=data['current_floor'],
                        vacant=data['vacant'],
                        demand_id=data['demand_id'])
    db.session.add(new_status)
    db.session.commit()
    return jsonify({'SUCESS': 'Status created'}), 200

@app.route('/data', methods=['GET'])
def process_data():
    estatus2 = aliased(EStatus)
    last_demand_id = db.session.execute(db.select(DAnalytics.demand_id).order_by(DAnalytics.demand_id.desc())).first()
    db.session.commit()

    if last_demand_id is None:
        status_diff = db.session.execute(db.select(EStatus.demand_id, EDemands.timestamp, func.strftime('%s', estatus2.timestamp) - func.strftime('%s', EStatus.timestamp), EDemands.demand_floor - EStatus.current_floor, EStatus.current_floor, EDemands.demand_floor)
        .select_from(EStatus, EDemands)
        .where(EStatus.id < estatus2.id)
        .where(EStatus.demand_id == estatus2.demand_id))
    else:
        status_diff = db.session.execute(db.select(EStatus.demand_id, EDemands.timestamp, func.strftime('%s', estatus2.timestamp) - func.strftime('%s', EStatus.timestamp), EDemands.demand_floor - EStatus.current_floor, EStatus.current_floor, EDemands.demand_floor)
        .distinct()
        .select_from(EStatus)
        .join(EDemands, EStatus.demand_id==EDemands.id)
        .where(EStatus.id < estatus2.id)
        .where(EStatus.demand_id > last_demand_id[0])
        .where(estatus2.demand_id > last_demand_id[0])
        .where(EStatus.demand_id == estatus2.demand_id)).fetchall()
    db.session.commit()
    for row in status_diff:
        new_analytics = DAnalytics(
            demand_id = row[0],
            demand_time = row[1],
            status_timediff = row[2],
            floordiff = row[3],
            resting_floor = row[4],
            end_floor = row[5]
        )
        db.session.add(new_analytics)
    db.session.commit()
    something = db.session.execute(db.select('*').select_from(DAnalytics)).fetchall()
    db.session.commit()
    def generate():
        for data in something:
            yield json.dumps(data._asdict())
    return stream_with_context(generate())

@app.route('/prqt', methods=['GET'])
def process_parquet():
    output = pd.DataFrame()
    something = db.session.execute(db.select('*').select_from(DAnalytics)).fetchall()
    db.session.commit()
    for data in something:
        df = pd.DataFrame([data._asdict()])
        output = pd.concat([output, df], ignore_index=True)

    table = pa.Table.from_pandas(output)
    pq.write_table(table, 'analytics.parquet')
    return send_file("analytics.parquet")