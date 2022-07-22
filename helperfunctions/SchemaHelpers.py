# Databricks notebook source
class SchemaHelpers():
    
    def __init__():
        import json
        return
    
    @staticmethod
    def getDDLString(structObj):
        import json
        ddl = []
        for c in json.loads(structObj.json()).get("fields"):

            name = c.get("name")
            dType = c.get("type")
            ddl.append(f"{name}::{dType} AS {name}")

        final_ddl = ", ".join(ddl)
        return final_ddl
    
    @staticmethod
    def getDDLList(structObj):
        import json
        ddl = []
        for c in json.loads(structObj.json()).get("fields"):

            name = c.get("name")
            dType = c.get("type")
            ddl.append(f"{name}::{dType} AS {name}")

        return ddl
    
    @staticmethod
    def getFlattenedSqlExprFromValueColumn(structObj):
        import json
        ddl = []
        for c in json.loads(structObj.json()).get("fields"):

            name = c.get("name")
            dType = c.get("type")
            ddl.append(f"value:{name}::{dType} AS {name}")

        return ddl
