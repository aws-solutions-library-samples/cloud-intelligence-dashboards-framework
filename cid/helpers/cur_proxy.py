import re
import logging

try:
    from itertools import batched #python3.12+
except ImportError:
    from itertools import islice
    def batched(iterable, n):
        iterator = iter(iterable)
        while batch := tuple(islice(iterator, n)):
            yield batch

logger = logging.getLogger(__name__)

cur1to2_mapping = {
    '''concat("year", '-' , "month")''': "billing_period",
    'year': """split_part("billing_period", '-', 1)""",
    'month': """split_part("billing_period", '-', 2)""",
    'identity_line_item_id': 'identity_line_item_id',
    'identity_time_interval': 'identity_time_interval',
    'pricing_lease_contract_length': 'pricing_lease_contract_length',
    'bill_invoice_id': 'bill_invoice_id',
    'bill_invoicing_entity': 'bill_invoicing_entity',
    'bill_billing_entity': 'bill_billing_entity',
    'pricing_offering_class': 'pricing_offering_class',
    'bill_bill_type': 'bill_bill_type',
    "savings_plan_start_time": "savings_plan_start_time",
    "savings_plan_end_time": "savings_plan_end_time",
    "reservation_start_time": "reservation_start_time",
    "reservation_end_time": "reservation_end_time",
    'bill_payer_account_id': 'bill_payer_account_id',
    'bill_billing_period_start_date': 'bill_billing_period_start_date',
    'bill_billing_period_end_date': 'bill_billing_period_end_date',
    'line_item_usage_account_id': 'line_item_usage_account_id',
    'line_item_line_item_type': 'line_item_line_item_type',
    'line_item_usage_start_date': 'line_item_usage_start_date',
    'line_item_usage_end_date': 'line_item_usage_end_date',
    'line_item_product_code': 'line_item_product_code',
    'line_item_usage_type': 'line_item_usage_type',
    'line_item_operation': 'line_item_operation',
    'line_item_availability_zone': 'line_item_availability_zone',
    'line_item_resource_id': 'line_item_resource_id',
    'line_item_usage_amount': 'line_item_usage_amount',
    'line_item_normalization_factor': 'line_item_normalization_factor',
    'line_item_normalized_usage_amount': 'line_item_normalized_usage_amount',
    'line_item_currency_code': 'line_item_currency_code',
    'line_item_unblended_rate': 'line_item_unblended_rate',
    'line_item_unblended_cost': 'line_item_unblended_cost',
    'line_item_blended_rate': 'line_item_blended_rate',
    'line_item_blended_cost': 'line_item_blended_cost',
    'line_item_line_item_description': 'line_item_line_item_description',
    'line_item_tax_type': 'line_item_tax_type',
    'line_item_legal_entity': 'line_item_legal_entity',
    'product_fee_code': 'product_fee_code',
    'product_fee_description': 'product_fee_description',
    'product_from_location': 'product_from_location',
    'product_from_location_type': 'product_from_location_type',
    'product_from_region_code': 'product_from_region_code',
    'product_instance_family': 'product_instance_family',
    'product_instance_type': 'product_instance_type',
    'product_location': 'product_location',
    'product_location_type': 'product_location_type',
    'product_operation': 'product_operation',
    'product_product_family': 'product_product_family',
    'product_region_code': 'product_region_code',
    'product_servicecode': 'product_servicecode',
    'product_sku': 'product_sku',
    'product_to_location': 'product_to_location',
    'product_to_location_type': 'product_to_location_type',
    'product_to_region_code': 'product_to_region_code',
    'product_usagetype': 'product_usagetype',
    'pricing_rate_code': 'pricing_rate_code',
    'pricing_rate_id': 'pricing_rate_id',
    'pricing_currency': 'pricing_currency',
    'pricing_public_on_demand_cost': 'pricing_public_on_demand_cost',
    'pricing_public_on_demand_rate': 'pricing_public_on_demand_rate',
    'pricing_term': 'pricing_term',
    'pricing_unit': 'pricing_unit',
    'pricing_purchase_option': 'pricing_purchase_option',
    'reservation_amortized_upfront_cost_for_usage': 'reservation_amortized_upfront_cost_for_usage',
    'reservation_amortized_upfront_fee_for_billing_period': 'reservation_amortized_upfront_fee_for_billing_period',
    'reservation_effective_cost': 'reservation_effective_cost',
    'reservation_modification_status': 'reservation_modification_status',
    'reservation_normalized_units_per_reservation': 'reservation_normalized_units_per_reservation',
    'reservation_number_of_reservations': 'reservation_number_of_reservations',
    'reservation_recurring_fee_for_usage': 'reservation_recurring_fee_for_usage',
    'reservation_subscription_id': 'reservation_subscription_id',
    'reservation_total_reserved_normalized_units': 'reservation_total_reserved_normalized_units',
    'reservation_total_reserved_units': 'reservation_total_reserved_units',
    'reservation_units_per_reservation': 'reservation_units_per_reservation',
    'reservation_unused_amortized_upfront_fee_for_billing_period': 'reservation_unused_amortized_upfront_fee_for_billing_period',
    'reservation_unused_normalized_unit_quantity': 'reservation_unused_normalized_unit_quantity',
    'reservation_unused_quantity': 'reservation_unused_quantity',
    'reservation_unused_recurring_fee': 'reservation_unused_recurring_fee',
    'reservation_upfront_value': 'reservation_upfront_value',
    'savings_plan_total_commitment_to_date': 'savings_plan_total_commitment_to_date',
    'savings_plan_savings_plan_a_r_n': 'savings_plan_savings_plan_a_r_n',
    'savings_plan_savings_plan_rate': 'savings_plan_savings_plan_rate',
    'savings_plan_used_commitment': 'savings_plan_used_commitment',
    'savings_plan_savings_plan_effective_cost': 'savings_plan_savings_plan_effective_cost',
    'savings_plan_amortized_upfront_commitment_for_billing_period': 'savings_plan_amortized_upfront_commitment_for_billing_period',
    'savings_plan_recurring_commitment_for_billing_period': 'savings_plan_recurring_commitment_for_billing_period',
    'savings_plan_offering_type': 'savings_plan_offering_type',
    'savings_plan_payment_option': 'savings_plan_payment_option',
    'savings_plan_purchase_term': 'savings_plan_purchase_term',
    'product_product_name': "product['product_name']",
    'product_alarm_type': "product['alarm_type']",
    'product_attachment_type': "product['attachment_type']",
    'product_availability': "product['availability']",
    'product_availability_zone': "product['availability_zone']",
    'product_capacitystatus': "product['capacitystatus']",
    'product_category': "product['category']",
    'product_ci_type': "product['ci_type']",
    'product_classicnetworkingsupport': "product['classicnetworkingsupport']",
    'product_clock_speed': "product['clock_speed']",
    'product_current_generation': "product['current_generation']",
    'product_database_engine': "product['database_engine']",
    'product_dedicated_ebs_throughput': "product['dedicated_ebs_throughput']",
    'product_deployment_option': "product['deployment_option']",
    'product_description': "product['description']",
    'product_durability': "product['durability']",
    'product_ecu': "product['ecu']",
    'product_engine_code': "product['engine_code']",
    'product_enhanced_networking_supported': "product['enhanced_networking_supported']",
    'product_event_type': "product['event_type']",
    'product_free_query_types': "product['free_query_types']",
    'product_group': "product['group']",
    'product_group_description': "product['group_description']",
    'product_instance_type_family': "product['instance_type_family']",
    'product_intel_avx2_available': "product['intel_avx2_available']",
    'product_intel_avx_available': "product['intel_avx_available']",
    'product_intel_turbo_available': "product['intel_turbo_available']",
    'product_license_model': "product['license_model']",
    'product_logs_destination': "product['logs_destination']",
    'product_marketoption': "product['marketoption']",
    'product_max_iops_burst_performance': "product['max_iops_burst_performance']",
    'product_max_iopsvolume': "product['max_iopsvolume']",
    'product_max_throughputvolume': "product['max_throughputvolume']",
    'product_max_volume_size': "product['max_volume_size']",
    'product_memory': "product['memory']",
    'product_message_delivery_frequency': "product['message_delivery_frequency']",
    'product_message_delivery_order': "product['message_delivery_order']",
    'product_min_volume_size': "product['min_volume_size']",
    'product_network_performance': "product['network_performance']",
    'product_normalization_size_factor': "product['normalization_size_factor']",
    'product_operating_system': "product['operating_system']",
    'product_physical_processor': "product['physical_processor']",
    'product_platopricingtype': "product['platopricingtype']",
    'product_platovolumetype': "product['platovolumetype']",
    'product_pre_installed_sw': "product['pre_installed_sw']",
    'product_processor_architecture': "product['processor_architecture']",
    'product_processor_features': "product['processor_features']",
    'product_queue_type': "product['queue_type']",
    'product_region': "product['region']",
    'product_request_type': "product['request_type']",
    'product_servicename': "product['servicename']",
    'product_storage': "product['storage']",
    'product_storage_class': "product['storage_class']",
    'product_storage_media': "product['storage_media']",
    'product_tenancy': "product['tenancy']",
    'reservation_reservation_a_r_n': 'reservation_reservation_a_r_n',
    'product_transfer_type': "product['transfer_type']",
    'product_vcpu': "product['vcpu']",
    'product_version': "product['version']",
    'product_volume_api_name': "product['volume_api_name']",
    'product_volume_type': "product['volume_type']",
    'product_vpcnetworkingsupport': "product['vpcnetworkingsupport']",
    'product_edition': "product['edition']",
    'product_gpu_memory': "product['gpu_memory']",
    'product_pack_size': "product['pack_size']",
    'product_q_present': "product['q_present']",
    'product_subscription_type': "product['subscription_type']",
    'product_usage_group': "product['usage_group']",
    'product_cache_engine': "product['cache_engine']",
    'product_invocation': "product['invocation']",
    'product_memory_gib': "product['memory_gib']",
    'product_time_window': "product['time_window']",
    'product_finding_group': "product['finding_group']",
    'product_finding_source': "product['finding_source']",
    'product_finding_storage': "product['finding_storage']",
    'product_standard_group': "product['standard_group']",
    'product_standard_storage': "product['standard_storage']",
    'product_product_type': "product['product_type']",
    'product_pricingplan': "product['pricingplan']",
    'product_provider': "product['provider']",
    'product_subservice': "product['subservice']",
    'product_type': "product['type']",
    'product_tickettype': "product['tickettype']",
    'product_memorytype': "product['memorytype']",
    'product_platousagetype': "product['platousagetype']",
    'product_with_active_users': "product['with_active_users']",
    'product_abd_instance_class': "product['abd_instance_class']",
    'product_size_flex': "product['size_flex']",
    'product_engine_major_version': "product['engine_major_version']",
    'product_extended_support_pricing_year': "product['extended_support_pricing_year']",
    '''concat('name', "bill_payer_account_id")''': 'bill_payer_account_name',
    '''concat('name', "line_item_usage_account_id")''': 'line_item_usage_account_name',
}

# Default columns are used as a basic set of columns when CUR is created. More can be added by views. For CUR1 and 2 these are different     ,,,,,,,,
default_columns = {
    '1': [
        "year",
        "month",
        "bill_bill_type",
        "bill_billing_entity",
        "bill_billing_period_end_date",
        "bill_billing_period_start_date",
        "bill_invoice_id",
        "bill_payer_account_id",
        "identity_line_item_id",
        "identity_time_interval",
        "line_item_availability_zone",
        "line_item_legal_entity",
        "line_item_line_item_description",
        "line_item_line_item_type",
        "line_item_operation",
        "line_item_product_code",
        "line_item_resource_id",
        "line_item_unblended_cost",
        "line_item_usage_account_id",
        "line_item_usage_amount",
        "line_item_usage_end_date",
        "line_item_usage_start_date",
        "line_item_usage_type",
        "pricing_lease_contract_length",
        "pricing_offering_class",
        "pricing_public_on_demand_cost",
        "pricing_purchase_option",
        "pricing_term",
        "pricing_unit",
        "product_cache_engine",
        "product_current_generation",
        "product_database_engine",
        "product_deployment_option",
        "product_from_location",
        "product_group",
        "product_instance_type",
        "product_instance_type_family",
        "product_license_model",
        "product_operating_system",
        "product_physical_processor",
        "product_processor_features",
        "product_product_family",
        "product_product_name",
        "product_region",
        "product_servicecode",
        "product_storage",
        "product_tenancy",
        "product_to_location",
        "product_volume_api_name",
        "product_volume_type",
        "reservation_amortized_upfront_fee_for_billing_period",
        "reservation_effective_cost",
        "reservation_end_time",
        "reservation_reservation_a_r_n",
        "reservation_start_time",
        "reservation_unused_amortized_upfront_fee_for_billing_period",
        "reservation_unused_recurring_fee",
        "savings_plan_amortized_upfront_commitment_for_billing_period",
        "savings_plan_end_time",
        "savings_plan_offering_type",
        "savings_plan_payment_option",
        "savings_plan_purchase_term",
        "savings_plan_savings_plan_a_r_n",
        "savings_plan_savings_plan_effective_cost",
        "savings_plan_start_time",
        "savings_plan_total_commitment_to_date",
        "savings_plan_used_commitment",
    ],
    '2': [
        "billing_period",
        "bill_bill_type",
        "bill_billing_entity",
        "bill_billing_period_end_date",
        "bill_billing_period_start_date",
        "bill_invoice_id",
        "bill_payer_account_id",
        "bill_payer_account_name",
        "identity_line_item_id",
        "identity_time_interval",
        "line_item_availability_zone",
        "line_item_legal_entity",
        "line_item_line_item_description",
        "line_item_line_item_type",
        "line_item_operation",
        "line_item_product_code",
        "line_item_resource_id",
        "line_item_unblended_cost",
        "line_item_usage_account_id",
        "line_item_usage_account_name",
        "line_item_usage_amount",
        "line_item_usage_end_date",
        "line_item_usage_start_date",
        "line_item_usage_type",
        "pricing_lease_contract_length",
        "pricing_offering_class",
        "pricing_public_on_demand_cost",
        "pricing_purchase_option",
        "pricing_term",
        "pricing_unit",
        "product['cache_engine']",
        "product['current_generation']",
        "product['database_engine']",
        "product['deployment_option']",
        "product_from_location",
        "product['group']",
        "product_instance_type",
        "product['instance_type_family']",
        "product['license_model']",
        "product['operating_system']",
        "product['physical_processor']",
        "product['processor_features']",
        "product_product_family",
        "product['product_name']",
        "product['region']",
        "product_servicecode",
        "product['storage']",
        "product['tenancy']",
        "product_to_location",
        "product['volume_api_name']",
        "product['volume_type']",
        "reservation_amortized_upfront_fee_for_billing_period",
        "reservation_effective_cost",
        "reservation_end_time",
        "reservation_reservation_a_r_n",
        "reservation_start_time",
        "reservation_unused_amortized_upfront_fee_for_billing_period",
        "reservation_unused_recurring_fee",
        "savings_plan_amortized_upfront_commitment_for_billing_period",
        "savings_plan_end_time",
        "savings_plan_offering_type",
        "savings_plan_payment_option",
        "savings_plan_purchase_term",
        "savings_plan_savings_plan_a_r_n",
        "savings_plan_savings_plan_effective_cost",
        "savings_plan_start_time",
        "savings_plan_total_commitment_to_date",
        "savings_plan_used_commitment",
    ],
}

# various types require various empty
empty = {
    'string': 'cast(null as varchar)',
    'varchar': 'cast(null as varchar)',
    'double': 'cast(null as double)',
    'timestamp': 'cast(null as timestamp)',
}

cur2_maps = {
    'product',
    'resource_tags',
    'cost_category',
    'discount',
}

class ProxyView():
    """ Proxy for CUR

    creates a proxy view for CUR
    """
    def __init__(self, cur, target_cur_version):
        self.cur = cur
        self.target_cur_version = target_cur_version
        self.current_cur_version = self.cur.version
        logger.debug(f'CUR proxy from {self.current_cur_version } to {self.target_cur_version }')
        self.fields_to_expose = list(set(default_columns[self.target_cur_version]))
        # add tags in cur2 proxy
        if cur.version.startswith('1') and target_cur_version.startswith('2') :
            for field in cur.tag_and_cost_category_fields:
                self.fields_to_expose.append(re.sub("(resource_tags|cost_category)_(.+)", r"\1['\2']", field))
        self.athena = self.cur.athena
        self.name = f'cur{self.target_cur_version}_proxy'
        self.exposed_fields = []
        self.exposed_maps = {}
        self.fields_to_expose_in_maps = {}
        self.fields_with_missing_requirements = [] # keep the list to show warning just once
        self.updated_once = False

    def read_from_athena(self):
        """ read the current state from athena. read all fields and their types from existing SQL view and also for each MAP read existing keys
        """
        # Read fields from the current view
        if not self.exposed_fields:
            exposed_fields_and_types = dict(self.athena.query(f'''
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_schema = '{self.athena.DatabaseName}'
                AND table_name = '{self.name}';
            '''))
            self.exposed_fields = list(exposed_fields_and_types.keys())
            logger.debug(f'exposed fields: {self.exposed_fields}')
        if not self.exposed_fields:
            return
        # If we have existing fields, we need to read also all map definitions from the SQL of the existing view
        def _parse_mapping_string(mapping_string):
            ''' read keys and values from definition
            ex:
                MAP(ARRAY['key1', 'key2'], ARRAY[value1, value2])
            '''
            arrays = re.findall(r'ARRAY\[(.*?)\]', mapping_string) # Extract arrays from the string
            keys = arrays[0].replace("'", '').split(',') # Split arrays into elements
            values = arrays[1].split(',') # FIXME: the values can be more complex then just columns and ma potentially include comma in some rare cases.
            return {key.strip(): value.strip() for key, value in zip(keys, values)}

        _current_sql = '\n'.join([line[0] for line in self.athena.query(f'SHOW CREATE VIEW {self.name}')])

        # transform: concat(ARRAY[a,b,c], ARRAY[d,e,f], ARRAY[e,g,h]) >> ARRAY[a,b,c,d,e,f,e,g,h]
        _current_sql = re.sub(r'concat\((.+?)\)', lambda match: match.group(1).replace('], ARRAY[', ','), _current_sql)
        for field in self.exposed_fields:
            if field in cur2_maps:
                if field not in self.exposed_maps:
                    self.exposed_maps[field] = set()
                maps = re.findall(fr'MAP\(ARRAY\[.+?\], ARRAY\[.+?\]\) {field}', _current_sql)
                if not maps:
                    logger.warning(f'Cannot find map {field} definition in the view {self.name}. It must be defined as MAP.')
                    continue
                current_map = _parse_mapping_string(maps[0])
                logger.debug(f'current definition of {field} = {current_map}')
                self.exposed_maps[field].update([key for key in current_map])
        logger.debug(f'exposed_maps = {self.exposed_maps}')

    def source_column_equivalents(self, field):
        """ Given a field of cur return an equivalent field in the target cur system. This one does not care if field actually exists
        field: target CUR field
        returns: source CUR SQL

        there can be more then one field
        """
        logger.trace(f'source_column_equivalents {field}')
        if field in self.cur.fields and not field.endswith('_time'): # Same field name is more then common case so try it first
            return [field]

        if self.current_cur_version.startswith('2') and self.target_cur_version.startswith('2'): # field from CUR2 to CUR2
            return [field] # all the same
        if self.current_cur_version.startswith('1') and self.target_cur_version.startswith('1'): # field from CUR1 to CUR1
            return [field] # all the same
        if self.current_cur_version.startswith('2') and self.target_cur_version.startswith('1'): # field from CUR1 to CUR2
            if field not in cur1to2_mapping: #check if this field is in mapping
                for cur2map in cur2_maps:
                    if field.startswith(cur2map + '_'):
                        return [cur2map]
                logger.warning(f"{field} not known field of CUR2. needs to be added in code. Please create a github issue")
            res = cur1to2_mapping.get(field, field)
            return self.get_fields_from_sql(res)
        if self.current_cur_version.startswith('1') and self.target_cur_version.startswith('2'): # field from CUR2 to CUR1
            if field.lower() in cur2_maps:
                return [] # field mapping itself without indication to concrete component
            matches = re.findall(r"(\w+)\['(\w+)'\]", field)
            if matches:
                map_field, key = matches[0]
                if map_field not in self.fields_to_expose_in_maps:
                    self.fields_to_expose_in_maps[map_field] = set()
                self.fields_to_expose_in_maps[map_field].add(key)
                logger.trace(f"ret {map_field}_{key}")
                return [f'{map_field}_{key}']

            #if field.endswith('_time'): # can be
            #    return self.get_fields_from_sql(field)

            cur2to1_mapping = {value: key for key, value in cur1to2_mapping.items()}
            if field not in cur2to1_mapping and field not in cur2_maps:
                logger.warning(f"The field '{field}' is not known field of CUR1. needs to be added in code. Please create a github issue")
            ret =  self.get_fields_from_sql(cur2to1_mapping.get(field, field)) 
            logger.trace(f"ret {ret}")
            return ret

    def get_fields_from_sql(self, field):
        """ get a sql field from an expression
        ex:
            '''concat('name', "bill_payer_account_id")''' => "bill_payer_account_id"
            parameters['aaa'] => parameters
            plane_fields => plane_fields
        """
        if '"' in field:
            matches = re.findall(r'"(.+_time)"', field)
            return matches
        if '[' in field:
            return [field.split('[')[0]]
        return [field]

    def get_sql_expression(self, field, field_type):
        """ Given a field of cur return an SQL representation of the field in the target cur system. Takes into account existence of fields in the current cur.
        field: target CUR field
        returns: CUR field SQL representation. Can be NULL
        """
        equivalents = self.source_column_equivalents(field) # Do not remove this. needed to populate self.fields_to_expose_in_maps
        missing_requirements = []
        for requirement in equivalents:
            if not self.cur.column_exists(requirement):
                missing_requirements.append(requirement)

        if missing_requirements and field_type.lower() in empty:
            logger.trace(f'field {field} cannot be present as prereqs are missing in source cur: {missing_requirements}')
            return empty[field_type.lower()]

        if field in self.cur.fields and not field.endswith('_time'): # Same field name is more then common case so try it first _date have different fields
            return field
        if self.current_cur_version.startswith('2') and self.target_cur_version.startswith('2'): # field from CUR2 to CUR2
            return field.split('[')[0]
        if self.current_cur_version.startswith('1') and self.target_cur_version.startswith('1'): # field from CUR1 to CUR2
            return field
        if self.current_cur_version.startswith('1') and self.target_cur_version.startswith('2'): # field from CUR1 to CUR2
            if field_type.lower().startswith('map'):
                map_field = field.split('[')[0]
                map_mapping = {}
                keys_set = set(self.exposed_maps.get(map_field, set()))
                keys_set.update(self.fields_to_expose_in_maps.get(map_field, set()))
                for key in keys_set:
                    map_field_key = f'{map_field}_{key}'
                    if self.cur.column_exists(map_field_key):
                        if map_field_key.encode('unicode-escape').decode('ascii') != map_field_key: # field name contains unicode characters that must be escaped
                            map_field_key = f'"{map_field_key}"'
                        map_mapping[key] = map_field_key
                    else:
                        map_mapping[key] = empty['string'] # all known maps have string values for now
                if not map_mapping:
                    return 'cast(NULL AS MAP<VARCHAR, VARCHAR>)'
                map_mapping = dict(sorted(map_mapping.items())) # ordered dict

                chunk_size = 254 # https://github.com/prestodb/presto/issues/9073
                key_arrays =   [f"""ARRAY[{', '.join(["'" + key + "'" for key in chunk])}]""" for chunk in batched(map_mapping.keys(), chunk_size)]
                value_arrays = [f"""ARRAY[{', '.join(chunk                             )}]""" for chunk in batched(map_mapping.values(), chunk_size)]
                return f'''
                    MAP(
                        {' || '.join(key_arrays)},
                        {' || '.join(value_arrays)}
                    )
                '''

            cur2to1_mapping = {value: key for key, value in cur1to2_mapping.items()}
            if field in cur2to1_mapping:
                return cur2to1_mapping[field]
            else:
                raise NotImplementedError(f'CUR1 field {field} has no known equivalent')
        if self.current_cur_version.startswith('2') and self.target_cur_version.startswith('1'):
            for tag_type in 'resource_tags', 'cost_category':
                if field.startswith(tag_type + '_'):
                    short_field_name = field[len(tag_type + '_'):]
                    return f"{tag_type}['{short_field_name}']"


    def column_surely_exist(self, field_to_expose):
        if not self.updated_once:
            return False
        target_field = self.get_fields_from_sql(field_to_expose)[0]
        if target_field not in self.exposed_fields:
            return False
        if '[' in  field_to_expose:
            key = field_to_expose.split('[')[1].split(']')[0].replace("'",'')
            if key not in self.exposed_maps.get(target_field.lower(),{}):
                return False
        return True

    def create_or_update_view(self):
        """ Create or update view
        """
        if all([self.column_surely_exist(field_to_expose) for field_to_expose in self.fields_to_expose]):
            logger.debug('no need for proxy change. skip.')
            return

        self.read_from_athena()
        all_target_fields  = sorted(list(set(self.exposed_fields + self.fields_to_expose)))
        logger.trace(f'all_target_fields = {all_target_fields}')
        lines = {}
        for field in all_target_fields:
            target_field = self.get_fields_from_sql(field)[0]
            target_field_type = self.cur.get_type_of_column(target_field, self.target_cur_version)
            logger.trace(f'get_sql_expression {field} {target_field_type}')
            mapped_expression = self.get_sql_expression(field, target_field_type) #
            logger.trace(f'cur_proxy: mapped_expression({field}) = {mapped_expression}')
            requirements = self.source_column_equivalents(target_field)
            missing_requirements = [r for r in requirements if not self.cur.column_exists(r)]
            if not missing_requirements:
                expression = mapped_expression                      # then take resulting expression as is
            else:
                if field not in self.fields_with_missing_requirements:
                    self.fields_with_missing_requirements.append(field)
                    logger.warning(f"Missing requirement for field {field}: {', '.join(missing_requirements)}. Setting as empty.")
                if target_field_type.lower() not in empty:
                    raise RuntimeError(f'{target_field_type} not in empty list for field {field}. Raise a github issue.')
                expression = empty.get(target_field_type.lower(), 'null')
            lines[target_field] = expression # for map we will take the latest
        select_block = '\n                ,'.join([f'{expression} {field}' for field, expression in sorted(lines.items())])
        query = (f'''
            CREATE OR REPLACE VIEW "{self.name}" AS
            SELECT
                {select_block}
            FROM
                "{self.cur.database}"."{self.cur.table_name}"
        ''')
        res = self.athena.create_or_update_view(self.name, query) # this cost 3 athena calls
        logging.debug(res)

        self.exposed_fields = all_target_fields

    def get_table_metadata(self):
        return self.athena.get_table_metadata(self.name)


def _experiment():
    ''' if you want to play with Proxy CUR you can run following code
    '''
    import boto3
    from cid.helpers.cur import ProxyCUR
    from cid.helpers import Athena,Glue
    from cid.helpers.cur import CUR
    cur = CUR(athena=Athena(session=boto3.session.Session()),glue=Glue(session=boto3.session.Session()))
    proxy = ProxyCUR(cur=cur, target_cur_version='2')

    