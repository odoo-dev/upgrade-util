from .data_wrappers import Spreadsheet, create_data_source_from_cmd
from .misc import adapt_view_link_cells, remove_lists, remove_odoo_charts, remove_pivots
from .revisions import CommandAdapter, Drop
from odoo.addons.base.maintenance.migrations import util


def modify_all_models(cr, data):
    spreadsheet = Spreadsheet(data)
    cells_adapters = ()
    revisions_adapters = ()

    to_remove = [model for model, new_model in util.ENVIRON["__renamed_models"].items() if not new_model]
    to_change = {old_model: new_model for old_model, new_model in util.ENVIRON["__renamed_models"].items() if new_model}

    # remove
    x, y = _remove_model_from_lists(to_remove, spreadsheet)
    cells_adapters += x
    revisions_adapters += y
    x, y = _remove_model_from_pivots(to_remove, spreadsheet)
    cells_adapters += x
    revisions_adapters += y
    x, y = _remove_model_from_charts(to_remove, spreadsheet)
    cells_adapters += x
    revisions_adapters += y
    x, y = _remove_model_from_filters(to_remove, spreadsheet)
    cells_adapters += x
    revisions_adapters += y
    x, y = _remove_model_from_view_link(to_remove, spreadsheet)
    cells_adapters += x
    revisions_adapters += y

    # rename
    x, y = _rename_model_in_list(spreadsheet, to_change)
    cells_adapters += x
    revisions_adapters += y
    x, y = _rename_model_in_pivot(spreadsheet, to_change)
    cells_adapters += x
    revisions_adapters += y
    x, y = _rename_model_in_filters(spreadsheet, to_change)
    cells_adapters += x
    revisions_adapters += y
    x, y = _rename_model_in_charts(spreadsheet, to_change)
    cells_adapters += x
    revisions_adapters += y
    x, y = _rename_model_in_view_link(spreadsheet, to_change)
    cells_adapters += x
    revisions_adapters += y

    # spreadsheet.clean_empty_cells()  ## TODO do only once ...

    return spreadsheet.data, cells_adapters, revisions_adapters


def _rename_model_in_charts(spreadsheet: Spreadsheet, model_change):
    for chart in spreadsheet.odoo_charts:
        if new := model_change.get(chart.model, False):
            chart.model = new

    def adapt_insert(cmd):
        if cmd["definition"]["type"].startswith("odoo_"):
            chart = create_data_source_from_cmd(cmd)
            if new := model_change.get(chart.model, False):
                chart.model = new

    return (), (CommandAdapter("CREATE_CHART", adapt_insert),)


def _rename_model_in_list(spreadsheet: Spreadsheet, model_change):
    for olist in spreadsheet.lists:
        if new := model_change.get(olist.model, False):
            olist.model = new

    def adapt_insert(cmd):
        olist = create_data_source_from_cmd(cmd)

        if new := model_change.get(olist.model, False):
            olist.model = new

    return (), (CommandAdapter("INSERT_ODOO_LIST", adapt_insert),)


def _rename_model_in_pivot(spreadsheet: Spreadsheet, model_change):
    for pivot in spreadsheet.pivots:
        if new := model_change.get(pivot.model, False):
            pivot.model = new

    def adapt_insert(cmd):
        pivot = create_data_source_from_cmd(cmd)
        if not pivot:
            return
        if new := model_change.get(pivot.model, False):
            pivot.model = new

    return (), (CommandAdapter("INSERT_PIVOT", adapt_insert),)


def _rename_model_in_filters(spreadsheet: Spreadsheet, model_change):
    def rename_relational_filter(gfilter):
        new_model = model_change.get(gfilter["modelName"], False)
        if gfilter["type"] == "relation" and new_model:
            gfilter["modelName"] = new_model

    for gfilter in spreadsheet.global_filters:
        rename_relational_filter(gfilter)

    def adapt_insert(cmd):
        rename_relational_filter(cmd["filter"])

    return (), (
        CommandAdapter("ADD_GLOBAL_FILTER", adapt_insert),
        CommandAdapter("EDIT_GLOBAL_FILTER", adapt_insert),
    )


def _rename_model_in_view_link(spreadsheet: Spreadsheet, model_change):
    def adapt_view_link(action):
        if new := model_change.get(action["modelName"], False):
            action["modelName"] = new

    return adapt_view_link_cells(spreadsheet, adapt_view_link)


def _remove_model_from_lists(models, spreadsheet: Spreadsheet):
    lists_to_delete = [olist.id for olist in spreadsheet.lists if olist.model in models]
    return remove_lists(
        spreadsheet,
        lists_to_delete,
        lambda olist: olist.model in models,  # check the olist rename from list
    )


def _remove_model_from_pivots(models, spreadsheet: Spreadsheet):
    pivots_to_delete = [pivot.id for pivot in spreadsheet.pivots if pivot.model in models]
    return remove_pivots(
        spreadsheet,
        pivots_to_delete,
        lambda pivot: pivot.model in models,
    )


def _remove_model_from_charts(models, spreadsheet: Spreadsheet):
    chart_to_delete = [chart.id for chart in spreadsheet.odoo_charts if chart.model in models]
    return remove_odoo_charts(
        spreadsheet,
        chart_to_delete,
        lambda chart: chart.model in models,
    )


def _remove_model_from_filters(models, spreadsheet: Spreadsheet):
    global_filters = spreadsheet.global_filters
    to_delete = [
        gFilter["id"] for gFilter in global_filters if gFilter["type"] == "relation" and gFilter["modelName"] in models
    ]
    spreadsheet.delete_global_filters(*to_delete)

    def adapt_edit_filter(cmd):
        if cmd["filter"]["id"] in to_delete:
            return Drop
        return cmd

    def adapt_add_filter(cmd):
        if cmd["filter"]["type"] == "relation" and cmd["filter"]["modelName"] in models:
            to_delete.append(cmd["filter"]["id"])
            return Drop
        return cmd

    def adapt_remove_filter(cmd):
        if cmd["id"] in to_delete:
            return Drop
        return cmd

    return (), (
        CommandAdapter("ADD_GLOBAL_FILTER", adapt_add_filter),
        CommandAdapter("EDIT_GLOBAL_FILTER", adapt_edit_filter),
        CommandAdapter("REMOVE_GLOBAL_FILTER", adapt_remove_filter),
    )


def _remove_model_from_view_link(models, spreadsheet: Spreadsheet):
    def adapt_view_link(action):
        if action["modelName"] in models:
            return Drop

    return adapt_view_link_cells(spreadsheet, adapt_view_link)
