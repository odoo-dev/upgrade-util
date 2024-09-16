import logging
import sys
import uuid
from concurrent.futures import ProcessPoolExecutor
from itertools import repeat
import time

import orjson

from odoo.sql_db import db_connect

from odoo.addons.base.maintenance.migrations import util
from odoo.addons.base.maintenance.migrations.util import spreadsheet

_logger = logging.getLogger(__name__)


def update_spreadsheets_table_changes(cr):
    # NOTE
    # `ProcessPoolExecutor.map` arguments needs to be pickleable
    # Functions can only be pickle if they are importable.
    # However, the current file is not importable due to the dash in the filename.
    # We should then put the executed function in its own importable file.
    name = f"_upgrade_{uuid.uuid4().hex}"
    # TODO need to make it relative OFC
    file_path = "/home/kelddun/rar-workspace/upgrade-util/src/util/spreadsheet/brol.py"
    brol = sys.modules[name] = util.import_script(file_path, name=name)
    update_documents(cr, brol)
    update_dashboards(cr, brol)
    update_templates(cr, brol)
    update_snapshots(cr, brol)


def get_revisions(cr, res_model, res_id):
    cr.execute(
        """
        SELECT id, commands
          FROM spreadsheet_revision
         WHERE res_model=%s
           AND res_id=%s
        """,
        [res_model, res_id],
    )
    return cr.fetchall()


def _update_json(cr, attachment_id, db_datas):
    data = orjson.loads(db_datas.tobytes())
    data, cells_model_adapters, model_adapters = spreadsheet.models.modify_all_models(cr, data)
    data, cells_fields_adapters, field_adapters = spreadsheet.fields.modify_all_fields(cr, data)

    ## apply cells field adapters to data
    cells_adapters = cells_model_adapters + cells_fields_adapters

    spreadsheetObj = spreadsheet.data_wrappers.Spreadsheet(data)
    for cell in spreadsheetObj.cells:  # <<<<  most costy shit i've ever seen
        # designeds to change in place ?
        apply_cells_adapters(cell, *cells_adapters)


    spreadsheetObj.clean_empty_cells()
    data = spreadsheetObj.data  # looks like a bad use of resource to recast all the fn time and useless??

    spreadsheet.misc.write_attachment(cr, attachment_id, data)
    return model_adapters + field_adapters


def _update_revisions(cr, res_model, res_id, *adapters):
    revisions_data = []
    revisions_ids = []
    for revision_id, commands in get_revisions(cr, res_model, res_id):
        revisions_data.append(orjson.loads(commands))
        revisions_ids.append(revision_id)

    revisions = spreadsheet.revisions.transform_revisions_data(revisions_data, *adapters)
    for revision_id, revision in zip(revisions_ids, revisions):
        cr.execute(
            """
            UPDATE spreadsheet_revision
                SET commands=%s
                WHERE id=%s
            """,
            [orjson.dumps(revision, option=orjson.OPT_NON_STR_KEYS).decode(), revision_id],
        )


def update_spreadsheet(dbname, res_model, res_id, attachment_id, update_revisions=True):
    cursor = db_connect(dbname).cursor
    with cursor() as cr:
        cr.execute(
            """
            SELECT db_datas FROM ir_attachment
            WHERE id=%s
        """,
            [attachment_id],
        )
        db_datas = cr.fetchone()[0]
        if not db_datas:
            print("naaaaa\n"*20)
            print(dbname, res_model, res_id, attachment_id)
        else: 
            print("cool works\n"*5)
            print(dbname, res_model, res_id, attachment_id)
            print(len(db_datas))
        if update_revisions:
            all_adapters = _update_json(cr, attachment_id, db_datas)
            _update_revisions(cr, res_model, res_id, *all_adapters)
        else:
            _update_json(cr, attachment_id, db_datas)
        cr.commit()


def update_spreadsheet_caca(dbname, res_model, res_id, attachment_id):
    cursor = db_connect(dbname).cursor
    with cursor() as cr:
        cr.execute(
            """
            SELECT db_datas FROM ir_attachment
            WHERE id=%s
        """,
            [attachment_id],
        )
        db_datas = cr.fetchone()[0]
        if not db_datas:
            print("naaaaa\n"*20)
            print(dbname, res_model, res_id, attachment_id)
        else: 
            print("cool works\n"*5)
            print(dbname, res_model, res_id, attachment_id)
            print(len(db_datas))
        _update_json(cr, attachment_id, db_datas)
        cr.commit();

def update_documents(cr, brol):
    if util.table_exists(cr, "documents_document"):
        with ProcessPoolExecutor() as executor:
            cr.execute(r"""
                SELECT doc.id AS document_id, a.id AS attachment_id
                  FROM documents_document doc
             LEFT JOIN ir_attachment a ON a.id = doc.attachment_id
                 WHERE doc.handler='spreadsheet'
                AND db_datas IS NOT null
                """)
            list(executor.map(brol.update_spreadsheet, repeat(cr.dbname), repeat("documents.document"), *zip(*cr.fetchall())))

def update_dashboards(cr, brol):
    if util.table_exists(cr, "spreadsheet_dashboard"):
        data_field = spreadsheet.misc._magic_spreadsheet_field(cr)  # "spreadsheet_binary_data" if version_gte("saas~16.3") else "data"
        with ProcessPoolExecutor() as executor:
            cr.execute(
                """
                SELECT res_id, id
                FROM ir_attachment
                WHERE res_model = 'spreadsheet.dashboard'
                AND res_field = %s
                AND db_datas IS NOT null
                """,
                [data_field],
            )
            list(executor.map(brol.update_spreadsheet, repeat(cr.dbname), repeat("spreadsheet.dashboard"), *zip(*cr.fetchall())))


def update_templates(cr, brol):
    if util.table_exists(cr, "documents_template"):
        with ProcessPoolExecutor() as executor:
            cr.execute(
                """
                SELECT res_id, id
                FROM ir_attachment
                WHERE res_model = 'spreadsheet.template'
                AND res_field = 'data'
                AND db_datas IS NOT null
                """,
            )
            list(executor.map(brol.update_spreadsheet, repeat(cr.dbname), repeat("spreadsheet.template"), *zip(*cr.fetchall())))


def update_snapshots(cr, brol):
    with ProcessPoolExecutor() as executor:
        cr.execute(r"""
            SELECT res_id, id
            FROM ir_attachment
            WHERE res_field = 'spreadsheet_snapshot'
            AND db_datas IS NOT null
        """)
        list(executor.map(brol.update_spreadsheet_caca, repeat(cr.dbname), repeat("unimportant"), *zip(*cr.fetchall())))


def apply_cells_adapters(cell, *adapters):
    for adapter in adapters:
        adapter(cell)


def upgrade_attachment_data(dbname, poubelle_id, attachment_id, upgrade_callback):
    cursor = db_connect(dbname).cursor
    with cursor() as cr:
        try:
            if not attachment_id:
                print("PUTAIN \n" *2)
                print(dbname, str(poubelle_id), str(attachment_id), str(upgrade_callback))

            cr.execute(
                """
                SELECT db_datas from ir_attachment
                WHERE id=%s
            """,
                [attachment_id],
            )
            db_datas = cr.fetchone()[0]
            data = orjson.loads(db_datas.tobytes())
            upgraded_data = upgrade_callback(spreadsheet.misc.load(data))

            cr.execute(
                """
                UPDATE ir_attachment
                    SET db_datas=%s
                    WHERE id=%s
                """,
                [orjson.dumps(upgraded_data, option=orjson.OPT_NON_STR_KEYS), attachment_id],
            )
        except Exception as e:
            _logger.error("c'est cassé cheh, %s" % str(e))
            raise e


def upgrade_documents(cr, brol, upgrade_callback, executor):
    if util.table_exists(cr, "documents_document"):
        cr.execute(r"""
            SELECT doc.id AS document_id, a.id AS attachment_id
                FROM documents_document doc
            LEFT JOIN ir_attachment a ON a.id = doc.attachment_id
                WHERE doc.handler='spreadsheet'
            """)
        return list(executor.map(brol.upgrade_attachment_data, repeat(cr.dbname), *zip(*cr.fetchall()), repeat(upgrade_callback)))

def upgrade_dashboards(cr, brol, upgrade_callback, executor):
    if util.table_exists(cr, "spreadsheet_dashboard"):
        data_field = spreadsheet.misc._magic_spreadsheet_field(cr)  # "spreadsheet_binary_data" if version_gte("saas~16.3") else "data"
        cr.execute(r"""
            SELECT res_id, id
                FROM ir_attachment
                WHERE res_model = 'spreadsheet.dashboard'
                AND res_field = %s
            """,
            [data_field],
        )
        return executor.map(brol.upgrade_attachment_data, repeat(cr.dbname), *zip(*cr.fetchall()), repeat(upgrade_callback))


def upgrade_templates(cr, brol, upgrade_callback, executor):
    if util.table_exists(cr, "spreadsheet_template"):
        data_field = spreadsheet.misc._magic_spreadsheet_field(cr)  # "spreadsheet_binary_data" if version_gte("saas~16.3") else "data"
        cr.execute("""
            SELECT res_id, id
                FROM ir_attachment
                WHERE res_model = 'spreadsheet.template'
                AND res_field = %s
                AND db_datas IS NOT null
            """,
            [data_field],
        )
        return executor.map(brol.upgrade_attachment_data, repeat(cr.dbname), *zip(*cr.fetchall()), repeat(upgrade_callback))

def upgrade_snapshots(cr, brol, upgrade_callback, executor):
    if util.table_exists(cr, "spreadsheet_template"):
        cr.execute(r"""
        SELECT 'snapshot', id
        FROM ir_attachment
        WHERE res_field = 'spreadsheet_snapshot'
        AND db_datas IS NOT null
        """)
        return executor.map(brol.upgrade_attachment_data, repeat(cr.dbname), *zip(*cr.fetchall()), repeat(upgrade_callback))

def upgrade_data(cr, upgrade_callback):
    # NOTE
    # `ProcessPoolExecutor.map` arguments needs to be pickleable
    # Functions can only be pickle if they are importable.
    # However, the current file is not importable due to the dash in the filename.
    # We should then put the executed function in its own importable file.
    name = f"_upgrade_{uuid.uuid4().hex}"
    # TODO need to make it relative OFC
    file_path = "/home/kelddun/rar-workspace/upgrade-util/src/util/spreadsheet/brol.py"
    brol = sys.modules[name] = util.import_script(file_path, name=name)
    with ProcessPoolExecutor(max_workers=4) as executor:
        upgrade_documents(cr, brol, upgrade_callback, executor)
        # upgrade_dashboards(cr, brol, upgrade_callback , executor)
        # upgrade_templates(cr, brol,upgrade_callback, executor)
        # upgrade_snapshots(cr, brol, upgrade_callback, executor)
