import json
import os
import re

from framework.infrastructure.utils.netweaver import GeneralUtil as util
from framework.infrastructure.utils.netweaver.RFCConnection import RFCConnection

fixed_mtid_in_systems = {
    "UK5": "3AV"
}

class SLTConfigurationOperation:

    def __init__(self):
        self.conn = RFCConnection.get_instance().conn

    def get_mass_transfer_id(self, version:str="V1", source:str="NONE",function_module="LTE2E_CREATE_SLT_CONFIG"):
        """Call FM via PyRFC connection and get MT-ID

        Parameters
        ----------
        :param function_module: string
            pass the function module what you want to call

        Returns
        ----------
        @return: tuple (mass transfer ID <class str>, mass transfer ID <class dict>)

        """

        # init default slt json config
        json_dict_data_slt_config = {
            "name": "automation_poc",
            "description": "automation_poc",
            "version": "V1",
            "calculationJobs": 1,
            "transferJobs": 1,
            "sourceRFC": "NONE"
        }
        json_dict_data_slt_config["sourceRFC"]=source
        
        if version is not None and version in ["V1", "GEN2"]:
            json_dict_data_slt_config["version"] = version

        # init del slt json config
        json_data_del_slt_config = {}

        # Covert slt config data from json to string
        json_str_slt_config = json.dumps(json_dict_data_slt_config)
        print("json_str_slt_config", json_str_slt_config)

        # Dict result like this: {'EV_RESULT': '{"MASSTRANSFERID":"2EL","SUCCESS":"Y","ERROR":"None"}'}
        # {'EV_RESULT': '{ "massTransferId":"023", "success":"Y", "error":"None"}'}

        # add try exception to handle the situation when the exception is 'The project ZIUUC_xxx already exists'
        fm_result = ""
        try:
            result = self.conn.call(function_module, IV_JSON_STR=json_str_slt_config)
            print("result", result)
            result = util.dict_capital_to_upper(result)

            # Get EV_RESULT str to dict
            fm_result = json.loads(result["EV_RESULT"])
            fm_result = util.dict_capital_to_upper(fm_result)
            print("fm_result", fm_result)
        except Exception as e:
            # add these code for https://sapjira.wdf.sap.corp/browse/DTDHQ220-735
            match_msg = "The project ZIUUC_... already exists"
            match_result = re.findall(match_msg, str(e.args))
            if len(match_result) > 0:
                print("Matched the message 'The project ZIUUC_... already exists'")
                # if tested system in pre-defined dict
                if os.getenv("CONN_SYSID") in fixed_mtid_in_systems.keys():
                    fm_result = {'MASSTRANSFERID': fixed_mtid_in_systems.get(os.getenv("CONN_SYSID")), 'SUCCESS': 'Y','ERROR': 'None'}
            else:
                raise Exception(e)

        # 2EL
        mt_id = fm_result["MASSTRANSFERID"]
        # jsonDataDelSltConfig {'massTransferId': '2EL'}
        json_data_del_slt_config["massTransferId"] = fm_result["MASSTRANSFERID"]

        return mt_id, json_data_del_slt_config

    def del_mass_transfer_id(self, json_data_del_slt_config):
        """ delete SLT configuration with passing mt-id"""

        if os.getenv("CONN_SYSID") in fixed_mtid_in_systems.keys() and json_data_del_slt_config.get("massTransferId") == fixed_mtid_in_systems.get(os.getenv("CONN_SYSID")):
            print("Ignore to delete fixed MTID")
        else:
            # <class 'str'>
            json_str_del_slt_config = json.dumps(json_data_del_slt_config)

            # dict result like this: {'EV_RESULT': '{"SUCCESS":"Y","ERROR":"None"}'}
            del_result = self.conn.call('LTE2E_DELETE_SLT_CONFIG', IV_JSON_STR=json_str_del_slt_config)
            print("del_mass_transfer_id - del_result", del_result)

            # Get EV_RESULT str to dict
            ev_result = json.loads(del_result["EV_RESULT"])
            result = util.dict_capital_to_upper(ev_result)
            print("del_mass_transfer_id - result", result)

            if "SUCCESS" in result and result["SUCCESS"] == "Y":
                print("Delete mass id successful")
            else:
                print("Delete mass id failed, please remove it manually")
