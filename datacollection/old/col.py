from app.models import FipsCode, Location, Facility
from datetime import datetime
from django.db import utils
from decimal import Decimal
from utils.utils import (get_census_block)
from azure.cosmos import exceptions, CosmosClient, PartitionKey
from django.conf import settings


class SDW_Importer(object):


    def __init__(self):
        # super().__init__()  # does python automatically call base class constructor when no init?
        self._cosmos_client = CosmosClient(
        settings.COSMOS.endpoint, settings.COSMOS.key)
        self._cosmos_database = _cosmos_client.get_database_client(
        settings.COSMOS.database_name)
        self._cosmos_container = _cosmos_database.get_container_client(
        settings.COSMOS.container_name)

    def _format_date(self, date_string):
        '''convert the dd/mm/yyyy format into a datetime object'''
        if date_string:
            return datetime.strptime(date_string, '%m/%d/%Y')
        return None

    def _format_int(self, int_string):
        '''deal with the annoying empty string to int exception'''
        if not int_string:
            return 0
        return int(int_string)

    def _format_decimal(self, decimal_string):
        '''deal with the annoying empty string exception'''
        if not decimal_string:
            return Decimal(0)
        return Decimal(decimal_string)

    def _get_fips_code(self, dfrow):
        clean_fips = lambda x: x if (
            x and len(x) == 5 and x != '00000') else None
        fips_candidates = (dfrow.FacFIPSCode, dfrow.FIPSCodes,
                           dfrow.FacDerivedStctyFIPS)
        fips_options = set(
            map(clean_fips, fips_candidates))

        fips_options.discard(None)
        # TODO what happens with multiples?
        if len(fips_options) > 1:
            print("found mulitiple fips codes %s for %s" %
                  (fips_options, dfrow.PWSId))
            raise AssertionError
        elif not len(fips_options): # let's calculate one (maybe do the same above?)
            print("found no valid fips codes %s for %s" %
                   (fips_options, dfrow.PWSId))
            return get_census_block(dfrow.FacLat, dfrow.FacLong)
        else:
            return fips_options.pop()


    def _get_zip_code(self, dfrow):
        if dfrow.FacZip:
            return dfrow.FacZip
        return dfrow.FacDerivedZip

    def update_cosmos_db(self, record):
        # https://docs.microsoft.com/en-us/python/api/azure-cosmos/azure.cosmos.cosmos_client?view=azure-python

        item = self._cosmos_container.upsert_item(record)
        request_charge = container.client_connection.last_response_headers['x-ms-request-charge']
        print('Read item with id {0}. Operation consumed {1} request units'.format(

        query="SELECT TOP 5 * FROM c WHERE c.PartitionKey = 'MN'"
        items_one_partition=list(container.query_items(
            query=query,
            enable_cross_partition_query=False
        ))

        items=list(container.query_items(
            query=query,
            enable_cross_partition_query=True
        ))

        request_charge=container.client_connection.last_response_headers['x-ms-request-charge']

        print('Query returned {0} items. Operation consumed {1} request units'.format(
            len(items), request_charge))


    def add_to_db(self, dfrow):
        # TODO need to import one or multiple
        if len(dfrow.SDWAIDs) > 9:
            # multiple water facilities included
            full_id_list = dfrow.SDWAIDs.split(' ')
            for sdwa_id in full_id_list:
                print('have a bunch of facs to add %s' %full_id_list)
                # facility.SDWAIDs = sdwa_id
        else:
            print('just one to add %s' % dfrow.PWSId)

        # TODO: here we calculate a score (what is current and historic?)

        fac = {
            "facility_name": dfrow.FacName,
            'pws_id': dfrow.PWSId,
            'registry_id': dfrow.RegistryID_x, #_x and _y because both data frames include this column in merge
            'current_violation_score': dfrow.Viopaccr, 
            'historic_violation_score': dfrow.Vioremain,
            'score': dfrow.Score,
            'facility_type': dfrow.PWSTypeCode,
            'population_served': dfrow.PopulationServedCount,
            'fips_code': self._get_fips_code(dfrow),
            'city': dfrow.FacCity,
            'zipcode': self._get_zip_code(dfrow),
            'county': dfrow.FacCounty,
            'latitude': dfrow.FacLat,
            'longitude': dfrow.FacLong,
            'state': dfrow.FacState,
            'street': dfrow.FacStreet
        }
        print('%s' %fac)
        # facility = Facility.objects.update_or_create(fac)
