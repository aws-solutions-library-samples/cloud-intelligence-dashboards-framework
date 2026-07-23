""" Unit tests for dataset-level taxonomy merging (cid/helpers/taxonomy_merger.py)

No AWS credentials or live connections required.
"""
from unittest.mock import patch

from cid.helpers.taxonomy_merger import (
    TaxonomyMerger,
    field_tokens,
    parse_merges,
    sanitize_name,
    serialize_merges,
)


def test_sanitize_name():
    assert sanitize_name('my tag:name') == 'my_tag_name'
    assert sanitize_name(' team ') == 'team'
    assert sanitize_name('cost-center') == 'cost_center'


def test_parse_merges_string():
    merges = parse_merges('team=tag_team+iam_principal_tag_team,bu=account_tag_bu+business_unit')
    assert merges == {
        'team': ['tag_team', 'iam_principal_tag_team'],
        'bu': ['account_tag_bu', 'business_unit'],
    }


def test_parse_merges_preserves_order():
    merges = parse_merges('team=b+a+c')
    assert merges['team'] == ['b', 'a', 'c']


def test_parse_merges_list():
    merges = parse_merges(['team=tag_team+account_tag_team'])
    assert merges == {'team': ['tag_team', 'account_tag_team']}


def test_parse_merges_invalid_groups_skipped():
    merges = parse_merges('noequalsign,=nofields,team=tag_team+account_tag_team,empty=')
    assert merges == {'team': ['tag_team', 'account_tag_team']}


def test_parse_merges_empty():
    assert parse_merges('') == {}
    assert parse_merges(None) == {}


def test_serialize_roundtrip():
    merges = {'team': ['tag_team', 'iam_principal_tag_team'], 'bu': ['business_unit']}
    assert parse_merges(serialize_merges(merges)) == merges


def test_field_tokens():
    assert field_tokens('iam_principal_tag_bedrock_iam_principal_Application') == ['bedrock', 'application']
    assert field_tokens('tag_bedrock_workspaces_application') == ['bedrock', 'workspaces', 'application']
    assert field_tokens('application') == ['application']
    assert field_tokens('cost_category_BillingTeam') == ['billingteam']


def test_source_label():
    merger = TaxonomyMerger(
        tag_fields=['tag_env', 'iam_principal_tag_team', 'account_tag_bu', 'user_attribute_tag_dept', 'cost_category_billing'],
        account_map_columns=['application'],
    )
    assert merger.source_label('tag_env') == 'resource tags'
    assert merger.source_label('iam_principal_tag_team') == 'IAM principal tags'
    assert merger.source_label('account_tag_bu') == 'account tags'
    assert merger.source_label('user_attribute_tag_dept') == 'user attributes'
    assert merger.source_label('cost_category_billing') == 'cost categories'
    assert merger.source_label('application') == 'account mapping'


def test_grouped_choices():
    merger = TaxonomyMerger(tag_fields=['tag_env', 'iam_principal_tag_team'], account_map_columns=['application'])
    assert merger.grouped_choices(['tag_env', 'iam_principal_tag_team', 'application']) == {
        'resource tags': ['tag_env'],
        'IAM principal tags': ['iam_principal_tag_team'],
        'account mapping': ['application'],
    }


def test_suggest_name_common_token():
    merger = TaxonomyMerger(tag_fields=[
        'iam_principal_tag_bedrock_iam_principal_Application',
        'tag_bedrock_workspaces_application',
    ])
    name = merger.suggest_name([
        'iam_principal_tag_bedrock_iam_principal_Application',
        'tag_bedrock_workspaces_application',
    ])
    assert name == 'bedrock_application_merged'


def test_suggest_name_avoids_taken():
    merger = TaxonomyMerger(tag_fields=['tag_application'], account_map_columns=['application_merged'])
    name = merger.suggest_name(['tag_application', 'application_merged'])
    # 'application_merged' exists as a column, suggestion must differ
    assert name not in merger.available_fields


def test_suggest_name_no_common_token_falls_back():
    merger = TaxonomyMerger(tag_fields=['tag_team', 'tag_env'])
    assert merger.suggest_name(['tag_team', 'tag_env']) == 'merged_tag_team'


def test_describe_merge():
    merger = TaxonomyMerger(tag_fields=['tag_env'], account_map_columns=['environment'])
    described = merger.describe_merge('env', ['tag_env', 'environment'])
    assert 'resource tag' in described
    assert ', else account mapping' in described


def test_find_example_group():
    merger = TaxonomyMerger(
        tag_fields=['tag_bedrock_workspaces_application', 'iam_principal_tag_bedrock_iam_principal_Application'],
        account_map_columns=['account_name'],
    )
    example = merger._find_example_group()
    assert example is not None
    assert len(example) == 2


def test_find_example_group_none_when_single_source():
    merger = TaxonomyMerger(tag_fields=['tag_team', 'tag_env'])
    assert merger._find_example_group() is None


def test_available_fields_dedup():
    merger = TaxonomyMerger(tag_fields=['tag_team'], account_map_columns=['business_unit', 'tag_team'])
    assert merger.available_fields == ['tag_team', 'business_unit']


def test_source_expression_tag_field():
    merger = TaxonomyMerger(tag_fields=['tag_team'], account_map_columns=['business_unit'])
    assert merger.source_expression('tag_team') == "parseJson(tags_json, '$.tag_team')"


def test_source_expression_account_map_column():
    merger = TaxonomyMerger(tag_fields=['tag_team'], account_map_columns=['business_unit'])
    assert merger.source_expression('business_unit') == '{business_unit}'


def test_build_merge_expression():
    merger = TaxonomyMerger(tag_fields=['tag_team', 'iam_principal_tag_team'], account_map_columns=['business_unit'])
    expression = merger.build_merge_expression(['tag_team', 'iam_principal_tag_team', 'business_unit'])
    assert expression == (
        "coalesce("
        "nullIf(parseJson(tags_json, '$.tag_team'), ''), "
        "nullIf(parseJson(tags_json, '$.iam_principal_tag_team'), ''), "
        "nullIf({business_unit}, ''))"
    )


def test_build_merge_expression_single_field_no_coalesce():
    merger = TaxonomyMerger(tag_fields=['tag_team'])
    assert merger.build_merge_expression(['tag_team']) == "nullIf(parseJson(tags_json, '$.tag_team'), '')"


def test_build_custom_fields():
    merger = TaxonomyMerger(tag_fields=['tag_team'], account_map_columns=['business_unit'])
    custom_fields = merger.build_custom_fields({'team': ['tag_team', 'business_unit']})
    assert list(custom_fields) == ['team']
    assert 'coalesce(' in custom_fields['team']


def test_build_custom_fields_filters_unavailable():
    """ a merge can reference account_map columns that exist only in some datasets """
    merger = TaxonomyMerger(tag_fields=['tag_team'])
    custom_fields = merger.build_custom_fields({'team': ['tag_team', 'business_unit']})
    assert custom_fields == {'team': "nullIf(parseJson(tags_json, '$.tag_team'), '')"}


def test_build_custom_fields_skips_empty_group():
    merger = TaxonomyMerger(tag_fields=['tag_team'])
    assert merger.build_custom_fields({'bu': ['business_unit', 'account_tag_bu']}) == {}


def test_build_custom_fields_skips_name_collision():
    merger = TaxonomyMerger(tag_fields=['tag_team', 'tag_env'])
    assert merger.build_custom_fields({'tag_team': ['tag_team', 'tag_env']}) == {}


@patch('cid.helpers.taxonomy_merger.unset_parameter')
@patch('cid.helpers.taxonomy_merger.set_parameters')
@patch('cid.helpers.taxonomy_merger.get_yesno_parameter', return_value=True)
@patch('cid.helpers.taxonomy_merger.all_yes', return_value=False)
@patch('cid.helpers.taxonomy_merger.isatty', return_value=True)
@patch('cid.helpers.taxonomy_merger.get_defaults', return_value={'taxonomy-merges': 'team=tag_team+business_unit'})
@patch('cid.helpers.taxonomy_merger.get_parameters', return_value={})
def test_resolve_merges_stored_config_kept(get_parameters_mock, get_defaults_mock, isatty_mock, all_yes_mock,
                                           get_yesno_mock, set_parameters_mock, unset_parameter_mock):
    """ config stored in Athena (loaded as defaults) is shown and kept on confirmation """
    merger = TaxonomyMerger(tag_fields=['tag_team'], account_map_columns=['business_unit'])
    assert merger.resolve_merges() == {'team': ['tag_team', 'business_unit']}
    # the kept config is set as a parameter so it is dumped back to Athena
    set_parameters_mock.assert_called_once_with({'taxonomy-merges': 'team=tag_team+business_unit'})


@patch('cid.helpers.taxonomy_merger.unset_parameter')
@patch('cid.helpers.taxonomy_merger.set_parameters')
@patch('cid.helpers.taxonomy_merger.get_yesno_parameter')
@patch('cid.helpers.taxonomy_merger.get_parameter')
@patch('cid.helpers.taxonomy_merger.cid_print')
@patch('cid.helpers.taxonomy_merger.all_yes', return_value=False)
@patch('cid.helpers.taxonomy_merger.isatty', return_value=True)
@patch('cid.helpers.taxonomy_merger.get_defaults', return_value={'taxonomy-merges': 'team=tag_team+gone_field'})
@patch('cid.helpers.taxonomy_merger.get_parameters', return_value={})
def test_resolve_merges_stored_config_with_unavailable_fields_reconfigures(
        get_parameters_mock, get_defaults_mock, isatty_mock, all_yes_mock,
        cid_print_mock, get_parameter_mock, get_yesno_mock,
        set_parameters_mock, unset_parameter_mock):
    """ stored config referencing gone fields is silently ignored: proceed as if no config exists """
    # reconfigure flow: user declines creating merges
    get_yesno_mock.return_value = False
    merger = TaxonomyMerger(tag_fields=['tag_team'], account_map_columns=['business_unit'])
    assert merger.resolve_merges() == {}
    # the stale config is not displayed to the user
    printed = ' '.join(str(c.args[0]) for c in cid_print_mock.call_args_list)
    assert 'previous run' not in printed
    # no 'keep configuration?' question was asked; the only yesno is the wizard entry question
    asked_params = [c.kwargs.get('param_name') or c.args[0] for c in get_yesno_mock.call_args_list]
    assert 'taxonomy-merge-keep' not in asked_params
    assert 'taxonomy-merge-create' in asked_params


@patch('cid.helpers.taxonomy_merger.get_yesno_parameter')
@patch('cid.helpers.taxonomy_merger.set_parameters')
@patch('cid.helpers.taxonomy_merger.all_yes', return_value=False)
@patch('cid.helpers.taxonomy_merger.isatty', return_value=False)
@patch('cid.helpers.taxonomy_merger.get_defaults', return_value={'taxonomy-merges': 'team=tag_team+business_unit'})
@patch('cid.helpers.taxonomy_merger.get_parameters', return_value={})
def test_resolve_merges_stored_config_non_interactive(get_parameters_mock, get_defaults_mock, isatty_mock,
                                                      all_yes_mock, set_parameters_mock, get_yesno_mock):
    """ non-interactive run reuses stored config without prompting """
    merger = TaxonomyMerger(tag_fields=['tag_team'], account_map_columns=['business_unit'])
    assert merger.resolve_merges() == {'team': ['tag_team', 'business_unit']}
    get_yesno_mock.assert_not_called()


@patch('cid.helpers.taxonomy_merger.get_parameters')
def test_resolve_merges_from_parameter(get_parameters_mock):
    get_parameters_mock.return_value = {'taxonomy-merges': 'team=tag_team+business_unit'}
    merger = TaxonomyMerger(tag_fields=['tag_team'], account_map_columns=['business_unit'])
    assert merger.resolve_merges() == {'team': ['tag_team', 'business_unit']}


@patch('cid.helpers.taxonomy_merger.get_parameters')
def test_resolve_merges_empty_parameter_disables(get_parameters_mock):
    """ --taxonomy-merges '' means explicitly no merges, no prompts """
    get_parameters_mock.return_value = {'taxonomy-merges': ''}
    merger = TaxonomyMerger(tag_fields=['tag_team'], account_map_columns=['business_unit'])
    assert merger.resolve_merges() == {}


@patch('cid.helpers.taxonomy_merger.isatty', return_value=False)
@patch('cid.helpers.taxonomy_merger.get_parameters', return_value={})
def test_resolve_merges_non_interactive_skips(get_parameters_mock, isatty_mock):
    merger = TaxonomyMerger(tag_fields=['tag_team'], account_map_columns=['business_unit'])
    assert merger.resolve_merges() == {}


@patch('cid.helpers.taxonomy_merger.isatty', return_value=True)
@patch('cid.helpers.taxonomy_merger.all_yes', return_value=False)
@patch('cid.helpers.taxonomy_merger.get_parameters', return_value={})
def test_resolve_merges_not_enough_fields(get_parameters_mock, all_yes_mock, isatty_mock):
    merger = TaxonomyMerger(tag_fields=['tag_team'])
    assert merger.resolve_merges() == {}


@patch('cid.helpers.taxonomy_merger.unset_parameter')
@patch('cid.helpers.taxonomy_merger.set_parameters')
@patch('cid.helpers.taxonomy_merger.get_yesno_parameter')
@patch('cid.helpers.taxonomy_merger.get_parameter')
@patch('cid.helpers.taxonomy_merger.all_yes', return_value=False)
@patch('cid.helpers.taxonomy_merger.isatty', return_value=True)
@patch('cid.helpers.taxonomy_merger.get_parameters', return_value={})
def test_resolve_merges_interactive(get_parameters_mock, isatty_mock, all_yes_mock,
                                    get_parameter_mock, get_yesno_mock, set_parameters_mock, unset_parameter_mock):
    # user: yes create merge, select 2 fields, name it 'team', no more groups
    get_yesno_mock.side_effect = [True, False]
    get_parameter_mock.side_effect = [
        ['tag_team', 'business_unit'],  # fields
        'team',                         # name
    ]
    merger = TaxonomyMerger(tag_fields=['tag_team'], account_map_columns=['business_unit'])
    merges = merger.resolve_merges()
    assert merges == {'team': ['tag_team', 'business_unit']}
    # choice is persisted in the taxonomy-merges parameter
    set_parameters_mock.assert_called_once_with({'taxonomy-merges': 'team=tag_team+business_unit'})


@patch('cid.helpers.taxonomy_merger.unset_parameter')
@patch('cid.helpers.taxonomy_merger.set_parameters')
@patch('cid.helpers.taxonomy_merger.get_yesno_parameter')
@patch('cid.helpers.taxonomy_merger.get_parameter')
@patch('cid.helpers.taxonomy_merger.all_yes', return_value=False)
@patch('cid.helpers.taxonomy_merger.isatty', return_value=True)
@patch('cid.helpers.taxonomy_merger.get_defaults', return_value={})
@patch('cid.helpers.taxonomy_merger.get_parameters', return_value={})
def test_resolve_merges_name_collision_reprompts(get_parameters_mock, get_defaults_mock, isatty_mock, all_yes_mock,
                                                 get_parameter_mock, get_yesno_mock,
                                                 set_parameters_mock, unset_parameter_mock):
    """ a name collision must not discard the selected fields: re-ask for the name """
    get_yesno_mock.side_effect = [True, False]
    get_parameter_mock.side_effect = [
        ['tag_application', 'application'],  # fields
        'application',                       # name collides with account_map column
        'app',                               # second attempt accepted
    ]
    merger = TaxonomyMerger(tag_fields=['tag_application'], account_map_columns=['application'])
    merges = merger.resolve_merges()
    assert merges == {'app': ['tag_application', 'application']}
    # the colliding answer was unset so the prompt could be asked again
    unset_parameter_mock.assert_any_call('taxonomy-merge-1-name')


@patch('cid.helpers.taxonomy_merger.unset_parameter')
@patch('cid.helpers.taxonomy_merger.set_parameters')
@patch('cid.helpers.taxonomy_merger.get_yesno_parameter')
@patch('cid.helpers.taxonomy_merger.get_parameter')
@patch('cid.helpers.taxonomy_merger.all_yes', return_value=False)
@patch('cid.helpers.taxonomy_merger.isatty', return_value=True)
@patch('cid.helpers.taxonomy_merger.get_parameters', return_value={})
def test_resolve_merges_used_fields_not_offered_again(get_parameters_mock, isatty_mock, all_yes_mock,
                                                      get_parameter_mock, get_yesno_mock,
                                                      set_parameters_mock, unset_parameter_mock):
    # user: yes create merge; group 1 takes tag_team+business_unit; wants another group,
    # but only 'environment' is left, so the loop stops without a second field prompt
    get_yesno_mock.side_effect = [True, True]
    get_parameter_mock.side_effect = [
        ['tag_team', 'business_unit'],  # group 1 fields
        'team',                         # group 1 name
    ]
    merger = TaxonomyMerger(tag_fields=['tag_team'], account_map_columns=['business_unit', 'environment'])
    merges = merger.resolve_merges()
    assert merges == {'team': ['tag_team', 'business_unit']}
    # first field prompt offered all fields; no second field prompt was made
    field_prompt_calls = [c for c in get_parameter_mock.call_args_list if 'fields' in c.args[0]]
    assert len(field_prompt_calls) == 1
    # choices are grouped by source: {group_label: [field, ...]}
    offered = [f for group in field_prompt_calls[0].kwargs['choices'].values() for f in group]
    assert offered == ['tag_team', 'business_unit', 'environment']


@patch('cid.helpers.taxonomy_merger.unset_parameter')
@patch('cid.helpers.taxonomy_merger.set_parameters')
@patch('cid.helpers.taxonomy_merger.get_yesno_parameter')
@patch('cid.helpers.taxonomy_merger.get_parameter')
@patch('cid.helpers.taxonomy_merger.all_yes', return_value=False)
@patch('cid.helpers.taxonomy_merger.isatty', return_value=True)
@patch('cid.helpers.taxonomy_merger.get_parameters', return_value={})
def test_resolve_merges_second_group_offers_remaining(get_parameters_mock, isatty_mock, all_yes_mock,
                                                      get_parameter_mock, get_yesno_mock,
                                                      set_parameters_mock, unset_parameter_mock):
    get_yesno_mock.side_effect = [True, True, False]
    get_parameter_mock.side_effect = [
        ['tag_team', 'iam_team'],        # group 1 fields
        'team',                          # group 1 name
        ['tag_env', 'environment'],      # group 2 fields
        'env',                           # group 2 name
    ]
    merger = TaxonomyMerger(tag_fields=['tag_team', 'iam_team', 'tag_env'], account_map_columns=['environment'])
    merges = merger.resolve_merges()
    assert merges == {'team': ['tag_team', 'iam_team'], 'env': ['tag_env', 'environment']}
    field_prompt_calls = [c for c in get_parameter_mock.call_args_list if 'fields' in c.args[0]]
    # second prompt must not offer fields already used in group 1
    offered = [f for group in field_prompt_calls[1].kwargs['choices'].values() for f in group]
    assert offered == ['tag_env', 'environment']


@patch('cid.helpers.taxonomy_merger.unset_parameter')
@patch('cid.helpers.taxonomy_merger.set_parameters')
@patch('cid.helpers.taxonomy_merger.get_yesno_parameter')
@patch('cid.helpers.taxonomy_merger.all_yes', return_value=False)
@patch('cid.helpers.taxonomy_merger.isatty', return_value=True)
@patch('cid.helpers.taxonomy_merger.get_parameters', return_value={})
def test_resolve_merges_user_declines(get_parameters_mock, isatty_mock, all_yes_mock,
                                      get_yesno_mock, set_parameters_mock, unset_parameter_mock):
    get_yesno_mock.return_value = False
    merger = TaxonomyMerger(tag_fields=['tag_team'], account_map_columns=['business_unit'])
    assert merger.resolve_merges() == {}
    # decline is persisted so the user is not asked again for the next dataset
    set_parameters_mock.assert_called_once_with({'taxonomy-merges': ''})
