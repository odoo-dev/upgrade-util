from .data_wrappers import Spreadsheet, create_data_source_from_cmd
from .misc import apply_in_all_spreadsheets, adapt_view_link_cells, remove_lists, remove_pivots, remove_odoo_charts
from .revisions import CommandAdapter, Drop, transform_revisions_data, transform_commands

from odoo.addons.base.maintenance.migrations import util


# def rename_model_in_all_spreadsheets(cr, old_value, new_value):
#     apply_in_all_spreadsheets(cr, old_value, (lambda data, revisions_data: rename_model(old_value, new_value, data, revisions_data)))

# TODO remove cr argument
# def rename_model(old, new, data, revisions = ()):
#     spreadsheet = Spreadsheet(data)
#     adapters = _rename_model_in_list(spreadsheet, old, new)
#     adapters += _rename_model_in_pivot(spreadsheet, old, new)
#     adapters += _rename_model_in_filters(spreadsheet, old, new)
#     adapters += _rename_model_in_charts(spreadsheet, old, new)
#     adapters += _rename_model_in_view_link(spreadsheet, old, new)
#     return spreadsheet.data, transform_revisions_data(revisions, *adapters)

# def remove_model_in_all_spreadsheets(cr, model):
#     apply_in_all_spreadsheets(cr, model, (lambda data, revisions_data: remove_model(model, data, revisions_data)))

# def remove_models(cr):
#     for model, new_model in util.ENVIRON["__renamed_models"].items():
#         if not new_model:
#             apply_in_all_spreadsheets(
#                     cr,
#                     model,
#                     (lambda data, revisions_data: remove_model(cr, model, data, revisions_data)),
#                 )
#         else:
#             apply_in_all_spreadsheets(
#                     cr,
#                     model,
#                     (lambda data, revisions_data: rename_model(cr, model, new_model, data, revisions_data)),
#                 )


def modify_all_models(cr, data, revisions=()):
    spreadsheet = Spreadsheet(data)
    adapters = ()

    to_remove = [model for model, new_model in util.ENVIRON["__renamed_models"].items() if not new_model]
    to_change = {old_model: new_model for old_model, new_model in util.ENVIRON["__renamed_models"].items() if new_model}

    adapters += _remove_model_from_lists(to_remove, spreadsheet)
    adapters += _remove_model_from_pivots(to_remove, spreadsheet)
    adapters += _remove_model_from_charts(to_remove, spreadsheet)
    adapters += _remove_model_from_filters(to_remove, spreadsheet)
    adapters += _remove_model_from_view_link(to_remove, spreadsheet)

    # rename
    adapters += _rename_model_in_list(spreadsheet, to_change)
    adapters += _rename_model_in_pivot(spreadsheet, to_change)
    adapters += _rename_model_in_filters(spreadsheet, to_change)
    adapters += _rename_model_in_charts(spreadsheet, to_change)
    adapters += _rename_model_in_view_link(spreadsheet, to_change)
    spreadsheet.clean_empty_cells()  ## TODO do only once ...

    return spreadsheet.data, transform_revisions_data(revisions, *adapters)


# def remove_model(model: str, data, revisions = ()) -> str:
#     spreadsheet = Spreadsheet(data)
#     adapters = _remove_model_from_lists(model, spreadsheet)
#     adapters += _remove_model_from_pivots(model, spreadsheet)
#     adapters += _remove_model_from_charts(model, spreadsheet)
#     adapters += _remove_model_from_filters(model, spreadsheet)
#     adapters += _remove_model_from_view_link(model, spreadsheet)
#     spreadsheet.clean_empty_cells()
#     return spreadsheet.data, transform_revisions_data(revisions, *adapters)

# def _rename_model_in_charts(spreadsheet: Spreadsheet, old, new):
#     for chart in spreadsheet.odoo_charts:
#         if chart.model == old:
#             chart.model = new

#     def adapt_insert(cmd):
#         if cmd["definition"]["type"].startswith("odoo_"):
#             chart = create_data_source_from_cmd(cmd)
#             if chart.model == old:
#                 chart.model = new

#     return (CommandAdapter("CREATE_CHART", adapt_insert),)


def _rename_model_in_charts(spreadsheet: Spreadsheet, model_change):
    for chart in spreadsheet.odoo_charts:
        if new := model_change.get(chart.model, False):
            chart.model = new

    def adapt_insert(cmd):
        if cmd["definition"]["type"].startswith("odoo_"):
            chart = create_data_source_from_cmd(cmd)
            if new := model_change.get(chart.model, False):
                chart.model = new

    return (CommandAdapter("CREATE_CHART", adapt_insert),)


# def _rename_model_in_list(spreadsheet: Spreadsheet, old, new):
#     for olist in spreadsheet.lists:
#         if olist.model == old:
#             olist.model = new

#     def adapt_insert(cmd):
#         olist = create_data_source_from_cmd(cmd)
#         if olist.model == old:
#             olist.model = new

#     return (CommandAdapter("INSERT_ODOO_LIST", adapt_insert),)


def _rename_model_in_list(spreadsheet: Spreadsheet, model_change):
    for olist in spreadsheet.lists:
        if new := model_change.get(olist.model, False):
            olist.model = new

    def adapt_insert(cmd):
        olist = create_data_source_from_cmd(cmd)

        if new := model_change.get(olist.model, False):
            olist.model = new

    return (CommandAdapter("INSERT_ODOO_LIST", adapt_insert),)


# def _rename_model_in_pivot(spreadsheet: Spreadsheet, old, new):
#     for pivot in spreadsheet.pivots:
#         if pivot.model == old:
#             pivot.model = new

#     def adapt_insert(cmd):
#         pivot = create_data_source_from_cmd(cmd)
#         if pivot.model == old:
#             pivot.model = new

#     return (CommandAdapter("INSERT_PIVOT", adapt_insert),)


def _rename_model_in_pivot(spreadsheet: Spreadsheet, model_change):
    for pivot in spreadsheet.pivots:
        if new := model_change.get(pivot.model, False):
            pivot.model = new

    def adapt_insert(cmd):
        pivot = create_data_source_from_cmd(cmd)
        if new := model_change.get(pivot.model, False):
            pivot.model = new

    return (CommandAdapter("INSERT_PIVOT", adapt_insert),)


# def _rename_model_in_filters(spreadsheet: Spreadsheet, old, new):
#     def rename_relational_filter(gfilter):
#         if gfilter["type"] == "relation" and gfilter["modelName"] == old:
#             gfilter["modelName"] = new

#     for gfilter in spreadsheet.global_filters:
#         rename_relational_filter(gfilter)

#     def adapt_insert(cmd):
#         rename_relational_filter(cmd["filter"])

#     return (
#         CommandAdapter("ADD_GLOBAL_FILTER", adapt_insert),
#         CommandAdapter("EDIT_GLOBAL_FILTER", adapt_insert),
#     )


def _rename_model_in_filters(spreadsheet: Spreadsheet, model_change):
    def rename_relational_filter(gfilter):
        new_model = model_change.get(gfilter["modelName"], False)
        if gfilter["type"] == "relation" and new_model:
            gfilter["modelName"] = new_model

    for gfilter in spreadsheet.global_filters:
        rename_relational_filter(gfilter)

    def adapt_insert(cmd):
        rename_relational_filter(cmd["filter"])

    return (
        CommandAdapter("ADD_GLOBAL_FILTER", adapt_insert),
        CommandAdapter("EDIT_GLOBAL_FILTER", adapt_insert),
    )


# def _rename_model_in_view_link(spreadsheet: Spreadsheet, old, new):
#     def adapt_view_link(action):
#         if action["modelName"] == old:
#             action["modelName"] = new

#     return adapt_view_link_cells(spreadsheet, adapt_view_link)


def _rename_model_in_view_link(spreadsheet: Spreadsheet, model_change):
    def adapt_view_link(action):
        if new := model_change.get(action["modelName"], False):
            action["modelName"] = new

    return adapt_view_link_cells(spreadsheet, adapt_view_link)


# def _remove_model_from_lists(model, spreadsheet: Spreadsheet):
#     lists_to_delete = [list.id for list in spreadsheet.lists if list.model == model]
#     return remove_lists(
#         spreadsheet,
#         lists_to_delete,
#         lambda list: list.model == model,
#     )


def _remove_model_from_lists(models, spreadsheet: Spreadsheet):
    lists_to_delete = [olist.id for olist in spreadsheet.lists if olist.model in models]
    return remove_lists(
        spreadsheet,
        lists_to_delete,
        lambda olist: olist.model in models,  # check the olist rename from list
    )


# def _remove_model_from_pivots(model, spreadsheet: Spreadsheet):
#     pivots_to_delete = [pivot.id for pivot in spreadsheet.pivots if pivot.model == model]
#     return remove_pivots(
#         spreadsheet,
#         pivots_to_delete,
#         lambda pivot: pivot.model == model,
#     )


def _remove_model_from_pivots(models, spreadsheet: Spreadsheet):
    pivots_to_delete = [pivot.id for pivot in spreadsheet.pivots if pivot.model in models]
    return remove_pivots(
        spreadsheet,
        pivots_to_delete,
        lambda pivot: pivot.model in models,
    )


# def _remove_model_from_charts(model, spreadsheet: Spreadsheet):
#     chart_to_delete = [chart.id for chart in spreadsheet.odoo_charts if chart.model == model]
#     return remove_odoo_charts(
#         spreadsheet,
#         chart_to_delete,
#         lambda chart: chart.model == model,
#     )


def _remove_model_from_charts(models, spreadsheet: Spreadsheet):
    chart_to_delete = [chart.id for chart in spreadsheet.odoo_charts if chart.model in models]
    return remove_odoo_charts(
        spreadsheet,
        chart_to_delete,
        lambda chart: chart.model in models,
    )


# def _remove_model_from_filters(model, spreadsheet: Spreadsheet):
#     global_filters = spreadsheet.global_filters
#     to_delete = [
#         gFilter["id"] for gFilter in global_filters if gFilter["type"] == "relation" and gFilter["modelName"] == model
#     ]
#     spreadsheet.delete_global_filters(*to_delete)

#     def adapt_edit_filter(cmd):
#         if cmd["filter"]["id"] in to_delete:
#             return Drop
#         return cmd

#     def adapt_add_filter(cmd):
#         if cmd["filter"]["type"] == "relation" and cmd["filter"]["modelName"] == model:
#             to_delete.append(cmd["filter"]["id"])
#             return Drop
#         return cmd

#     def adapt_remove_filter(cmd):
#         if cmd["id"] in to_delete:
#             return Drop
#         return cmd

#     return (
#         CommandAdapter("ADD_GLOBAL_FILTER", adapt_add_filter),
#         CommandAdapter("EDIT_GLOBAL_FILTER", adapt_edit_filter),
#         CommandAdapter("REMOVE_GLOBAL_FILTER", adapt_remove_filter),
#     )


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

    return (
        CommandAdapter("ADD_GLOBAL_FILTER", adapt_add_filter),
        CommandAdapter("EDIT_GLOBAL_FILTER", adapt_edit_filter),
        CommandAdapter("REMOVE_GLOBAL_FILTER", adapt_remove_filter),
    )


# def _remove_model_from_view_link(model, spreadsheet: Spreadsheet):
#     def adapt_view_link(action):
#         if action["modelName"] == model:
#             return Drop

#     return adapt_view_link_cells(spreadsheet, adapt_view_link)


def _remove_model_from_view_link(models, spreadsheet: Spreadsheet):
    def adapt_view_link(action):
        if action["modelName"] in models:
            return Drop

    return adapt_view_link_cells(spreadsheet, adapt_view_link)
