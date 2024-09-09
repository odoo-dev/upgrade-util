import logging
from concurrent.futures import ProcessPoolExecutor
import sys
import uuid
import orjson


from .data_wrappers import Spreadsheet
from .fields import modify_all_fields
from .misc import _magic_spreadsheet_field, write_attachment
from .models import modify_all_models
from .revisions import transform_revisions_data
from odoo.addons.base.maintenance.migrations import util

from psycopg2.extras import execute_batch


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
    update_dashboards(cr)
    update_templates(cr)
    update_snapshots(cr)


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


def _update_data(cr, attachment_id, db_datas):
    data = orjson.loads(db_datas.tobytes())

    data, cells_model_adapters, model_adapters = modify_all_models(cr, data)
    data, cells_fields_adapters, field_adapters = modify_all_fields(cr, data)

    ## apply cells field adapters to data
    cells_adapters = cells_model_adapters + cells_fields_adapters

    spreadsheet = Spreadsheet(data)
    for cell in spreadsheet.cells:  # <<<<  most costy shit i've ever seen
        # designeds to change in place ?
        apply_cells_adapters(cell, *cells_adapters)

    spreadsheet.clean_empty_cells()
    data = spreadsheet.data  # looks like a bad use of resource to recast all the fn time and useless??

    write_attachment(cr, attachment_id, data)
    return model_adapters + field_adapters


def _update_revisions(cr, res_model, res_id, *adapters):
    revisions_data = []
    revisions_ids = []
    for revision_id, commands in get_revisions(cr, res_model, res_id):
        revisions_data.append(orjson.loads(commands))
        revisions_ids.append(revision_id)

    revisions = transform_revisions_data(revisions_data, *adapters)
    for revision_id, revision in zip(revisions_ids, revisions):
        cr.execute(
            """
            UPDATE spreadsheet_revision
                SET commands=%s
                WHERE id=%s
            """,
            [orjson.dumps(revision, option=orjson.OPT_NON_STR_KEYS).decode(), revision_id],
        )


# def update_document(cr, doc_id, attachment_id):

#     all_adapters = _update_data(cr, attachment_id, db_datas)
#     _update_revisions(cr, "documents.document", doc_id, *all_adapters)

def update_documents(cr, brol):
    if util.table_exists(cr, "documents_document"):
        # with ProcessPoolExecutor() as executor, util.named_cursor(cr) as ncr:
        #     ncr.execute(r"""
        #         SELECT doc.id AS document_id, a.id AS attachment_id, a.db_datas
        #           FROM documents_document doc
        #      LEFT JOIN ir_attachment a ON a.id = doc.attachment_id
        #          WHERE doc.handler='spreadsheet'
        #         """
        #     )
        #     while chunk := ncr.fetchmany(100):  # fetchall() could cause MemoryError
        #         chunksize = 1000
        #         execute_batch(
        #             cr._obj,
        #             "UPDATE documents_document SET file_extension = %s WHERE id = %s",
        #             executor.map(brol.extract_extension, chunk, chunksize=chunksize),
        #             page_size=chunksize,
        #         )

        query = r"""
            SELECT doc.id AS document_id, a.id AS attachment_id, a.db_datas
              FROM documents_document doc
         LEFT JOIN ir_attachment a ON a.id = doc.attachment_id
             WHERE doc.handler='spreadsheet'
               AND id in %s
            """

        # TODO there are excel files in there! -> they should not be processed
        with util.named_cursor(cr, 20) as ncr:
            ncr.execute(query)

            for doc_id, attachment_id, db_datas in ncr:
                if db_datas:
                    all_adapters = _update_data(cr, attachment_id, db_datas)
                    _update_revisions(cr, "documents.document", doc_id, *all_adapters)


def update_dashboards(cr):
    if util.table_exists(cr, "spreadsheet_dashboard"):
        data_field = _magic_spreadsheet_field(cr)  # "spreadsheet_binary_data" if version_gte("saas~16.3") else "data"
        with util.named_cursor(cr, 20) as ncr:
            ncr.execute(
                """
                SELECT res_id, id, db_datas
                FROM ir_attachment
                WHERE res_model = 'spreadsheet.dashboard'
                AND res_field = %s
                """,
                [data_field],
            )
            for template_id, attachment_id, db_datas in ncr:
                if db_datas:
                    all_adapters = _update_data(cr, attachment_id, db_datas)
                    _update_revisions(cr, "spreadsheet.dashboard", template_id, *all_adapters)


def update_templates(cr):
    if util.table_exists(cr, "spreadsheet_template"):
        with util.named_cursor(cr, 20) as ncr:
            ncr.execute(
                """
                SELECT res_id, id, db_datas
                FROM ir_attachment
                WHERE res_model = 'spreadsheet.template'
                AND res_field = 'data'
                """,
            )
            for template_id, attachment_id, db_datas in ncr:
                if db_datas:
                    all_adapters = _update_data(cr, attachment_id, db_datas)
                    _update_revisions(cr, "spreadsheet.template", template_id, *all_adapters)


def update_snapshots(cr):
    query = r"""
         SELECT id, db_datas
           FROM ir_attachment
          WHERE res_field = 'spreadsheet_snapshot'
        """
    with util.named_cursor(cr, 20) as ncr:
        ncr.execute(query)

        for attachment_id, db_datas in ncr:
            if db_datas:
                _update_data(cr, attachment_id, db_datas)


def apply_cells_adapters(cell, *adapters):
    for adapter in adapters:
        adapter(cell)
