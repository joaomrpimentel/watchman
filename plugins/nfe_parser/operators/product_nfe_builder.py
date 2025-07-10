class Product_NfeBuilder:
    def __init__(self):
        self.data = {
            'nfe': {},
            'emitente': {},
            'emitente_endereco': {},
            'destinatario': {},
            'destinatario_endereco': {},
            'transportadora': {},
            'transportadora_endereco': {},
            'itens': [],
            'totais': {},
            'transporte': {},
            'volumes': [],
            'informacoes_adicionais': {}
        }

    def set_nfe(self, infNFe, ide):
        dhEmi_str = ide.findtext('nfe:dhEmi', '', self.ns)
        dt_obj_emi = self.parse_date_to_timestamp(dhEmi_str)
        dhSaiEnt_str = ide.findtext('nfe:dhSaiEnt', '', self.ns)
        dt_obj_sai = self.parse_date_to_timestamp(dhSaiEnt_str)

        if infNFe is not None:
            self.data['nfe']['chave_acesso'] = infNFe.attrib.get('Id', '')[3:]
            self.data['nfe']['versao'] = infNFe.attrib.get('versao', '')

        if ide is not None:
            self.data['nfe']['codigo_uf'] = int(ide.findtext('nfe:cUF', '0', self.ns))
            self.data['nfe']['codigo_nf'] = ide.findtext('nfe:cNF', '', self.ns)
            self.data['nfe']['natureza_operacao'] = ide.findtext('nfe:natOp', '', self.ns)
            self.data['nfe']['indicador_pagamento'] = self._safe_int(ide.findtext('nfe:indPag', '9', self.ns))
            self.data['nfe']['modelo'] = int(ide.findtext('nfe:mod', '0', self.ns))
            self.data['nfe']['serie'] = int(ide.findtext('nfe:serie', '0', self.ns))
            self.data['nfe']['numero'] = int(ide.findtext('nfe:nNF', '0', self.ns))        
            self.data['nfe']['data_emissao'] = dt_obj_emi.date() if dt_obj_emi else None
            self.data['nfe']['data_saida_entrada'] = dt_obj_sai.date() if dt_obj_sai else None
            self.data['nfe']['tipo_nf'] = int(ide.findtext('nfe:tpNF', '0', self.ns))
            self.data['nfe']['codigo_municipio_fato_gerador'] = ide.findtext('nfe:cMunFG', '', self.ns)
            self.data['nfe']['tipo_impressao'] = int(ide.findtext('nfe:tpImp', '0', self.ns))
            self.data['nfe']['tipo_emissao'] = int(ide.findtext('nfe:tpEmis', '0', self.ns))
            self.data['nfe']['digito_verificador'] = int(ide.findtext('nfe:cDV', '0', self.ns))
            self.data['nfe']['ambiente'] = int(ide.findtext('nfe:tpAmb', '0', self.ns))
            self.data['nfe']['finalidade_nf'] = int(ide.findtext('nfe:finNFe', '0', self.ns))
            self.data['nfe']['processo_emissao'] = int(ide.findtext('nfe:procEmi', '0', self.ns))
            self.data['nfe']['versao_processo'] = ide.findtext('nfe:verProc', '', self.ns)

        return self


    def set_emitente(self, emit):
        if emit is not None:
            self.data['emitente']['tipo_pessoa'] = 'EMITENTE'
            self.data['emitente']['cnpj'] = emit.findtext('nfe:CNPJ', None, self.ns)
            self.data['emitente']['cpf'] = emit.findtext('nfe:CPF', None, self.ns)
            self.data['emitente']['nome'] = emit.findtext('nfe:xNome', None, self.ns)
            self.data['emitente']['nome_fantasia'] = emit.findtext('nfe:xFant', None, self.ns)
            self.data['emitente']['inscricao_estadual'] = emit.findtext('nfe:IE', None, self.ns)
            self.data['emitente']['regime_tributario'] = self._safe_int(emit.findtext('nfe:CRT', '0', self.ns))

        return self

    def set_emitente_endereco(self, enderEmit):
        if enderEmit is not None:
            self.data['emitente_endereco']['tipo_endereco'] = 'PRINCIPAL'
            self.data['emitente_endereco']['logradouro'] = enderEmit.findtext('nfe:xLgr', '', self.ns)
            self.data['emitente_endereco']['numero'] = enderEmit.findtext('nfe:nro', '', self.ns)
            self.data['emitente_endereco']['complemento'] = enderEmit.findtext('nfe:xCpl', None, self.ns)
            self.data['emitente_endereco']['bairro'] = enderEmit.findtext('nfe:xBairro', '', self.ns)
            self.data['emitente_endereco']['codigo_municipio'] = enderEmit.findtext('nfe:cMun', '', self.ns)
            self.data['emitente_endereco']['municipio'] = enderEmit.findtext('nfe:xMun', '', self.ns)
            self.data['emitente_endereco']['uf'] = enderEmit.findtext('nfe:UF', '', self.ns)
            self.data['emitente_endereco']['cep'] = enderEmit.findtext('nfe:CEP', '', self.ns)
            self.data['emitente_endereco']['codigo_pais'] = enderEmit.findtext('nfe:cPais', None, self.ns)
            self.data['emitente_endereco']['pais'] = enderEmit.findtext('nfe:xPais', None, self.ns)
            self.data['emitente_endereco']['telefone'] = enderEmit.findtext('nfe:fone', None, self.ns)
        
        return self

    def set_destinatario(self, dest):
        if dest is not None:
            self.data['destinatario']['tipo_pessoa'] = 'DESTINATARIO'
            self.data['destinatario']['cnpj'] = dest.findtext('nfe:CNPJ', None, self.ns)
            self.data['destinatario']['cpf'] = dest.findtext('nfe:CPF', None, self.ns)
            self.data['destinatario']['nome'] = dest.findtext('nfe:xNome', '', self.ns)
            self.data['destinatario']['email'] = dest.findtext('nfe:email', None, self.ns)

        return self

    def set_destinatario_endereco(self, enderDest):
        if enderDest is not None:
            self.data['destinatario_endereco']['tipo_endereco'] = 'PRINCIPAL'
            self.data['destinatario_endereco']['logradouro'] = enderDest.findtext('nfe:xLgr', '', self.ns)
            self.data['destinatario_endereco']['numero'] = enderDest.findtext('nfe:nro', '', self.ns)
            self.data['destinatario_endereco']['complemento'] = enderDest.findtext('nfe:xCpl', None, self.ns)
            self.data['destinatario_endereco']['bairro'] = enderDest.findtext('nfe:xBairro', '', self.ns)
            self.data['destinatario_endereco']['codigo_municipio'] = enderDest.findtext('nfe:cMun', '', self.ns)
            self.data['destinatario_endereco']['municipio'] = enderDest.findtext('nfe:xMun', '', self.ns)
            self.data['destinatario_endereco']['uf'] = enderDest.findtext('nfe:UF', '', self.ns)
            self.data['destinatario_endereco']['cep'] = enderDest.findtext('nfe:CEP', '', self.ns)
        
        return self

    def add_items(self, items):
        for i, det in enumerate(items):
            item_nfe = {'numero_item': int(det.attrib.get('nItem', str(i + 1)))}

            prod = det.find('nfe:prod', self.ns)
            if prod is not None:
                item_nfe.update({
                    'codigo_produto': prod.findtext('nfe:cProd', '', self.ns),
                    'gtin': prod.findtext('nfe:cEAN', None, self.ns),
                    'descricao': prod.findtext('nfe:xProd', '', self.ns),
                    'ncm': prod.findtext('nfe:NCM', None, self.ns),
                    'cfop': prod.findtext('nfe:CFOP', '', self.ns),
                    'unidade_comercial': prod.findtext('nfe:uCom', '', self.ns),
                    'quantidade_comercial': float(prod.findtext('nfe:qCom', '0.0', self.ns)),
                    'valor_unitario_comercial': float(prod.findtext('nfe:vUnCom', '0.0', self.ns)),
                    'valor_total_bruto': float(prod.findtext('nfe:vProd', '0.0', self.ns)),
                    'gtin_tributavel': prod.findtext('nfe:cEANTrib', None, self.ns),
                    'unidade_tributavel': prod.findtext('nfe:uTrib', '', self.ns),
                    'quantidade_tributavel': float(prod.findtext('nfe:qTrib', '0.0', self.ns)),
                    'valor_unitario_tributavel': float(prod.findtext('nfe:vUnTrib', '0.0', self.ns)),
                })

            imposto_data = []
            imposto_node = det.find('nfe:imposto', self.ns)
            if imposto_node:
                icms_node = imposto_node.find('nfe:ICMS', self.ns)
                if icms_node is not None and len(icms_node) > 0:
                    icms_type_node = icms_node[0]
                    item_nfe['origem_mercadoria'] = self._safe_int(icms_type_node.findtext('nfe:orig', None, self.ns))
                    imposto_entry = {
                        'tipo_imposto': 'ICMS',
                        'origem': item_nfe['origem_mercadoria'],
                        'cst': icms_type_node.findtext('nfe:CST', None, self.ns),
                        'modalidade_base_calculo': self._safe_int(icms_type_node.findtext('nfe:modBC', None, self.ns)),
                        'valor_base_calculo': self._safe_float(icms_type_node.findtext('nfe:vBC', None, self.ns)),
                        'aliquota_percentual': self._safe_float(icms_type_node.findtext('nfe:pICMS', None, self.ns)),
                        'valor': self._safe_float(icms_type_node.findtext('nfe:vICMS', None, self.ns)),
                        'percentual_reducao_base_calculo': self._safe_float(icms_type_node.findtext('nfe:pRedBC', None, self.ns)),
                    }
                    imposto_data.append(imposto_entry)

                ipi_node = imposto_node.find('nfe:IPI', self.ns)
                if ipi_node is not None and len(ipi_node) > 0:
                    ipi_type_node = ipi_node.find('*', self.ns)
                    imposto_entry = {
                        'tipo_imposto': 'IPI',
                        'cst': ipi_type_node.findtext('nfe:CST', None, self.ns) if ipi_type_node is not None else None,
                        'codigo_enquadramento': ipi_node.findtext('nfe:cEnq', None, self.ns),
                    }
                    imposto_data.append(imposto_entry)

                pis_node = imposto_node.find('nfe:PIS', self.ns)
                if pis_node is not None and len(pis_node) > 0:
                    pis_type_node = pis_node[0]
                    imposto_entry = {
                        'tipo_imposto': 'PIS',
                        'cst': pis_type_node.findtext('nfe:CST', None, self.ns),
                        'valor_base_calculo': self._safe_float(pis_type_node.findtext('nfe:vBC', None, self.ns)),
                        'aliquota_percentual': self._safe_float(pis_type_node.findtext('nfe:pPIS', None, self.ns)),
                        'valor': self._safe_float(pis_type_node.findtext('nfe:vPIS', None, self.ns)),
                    }
                    imposto_data.append(imposto_entry)

                cofins_node = imposto_node.find('nfe:COFINS', self.ns)
                if cofins_node is not None and len(cofins_node) > 0:
                    cofins_type_node = cofins_node[0]
                    imposto_entry = {
                        'tipo_imposto': 'COFINS',
                        'cst': cofins_type_node.findtext('nfe:CST', None, self.ns),
                        'valor_base_calculo': self._safe_float(cofins_type_node.findtext('nfe:vBC', None, self.ns)),
                        'aliquota_percentual': self._safe_float(cofins_type_node.findtext('nfe:pCOFINS', None, self.ns)),
                        'valor': self._safe_float(cofins_type_node.findtext('nfe:vCOFINS', None, self.ns)),
                    }
                    imposto_data.append(imposto_entry)

            item_nfe['impostos'] = imposto_data
            self.data['itens'].append(item_nfe)
        return self

    def set_totais(self, total_node):
        if total_node is not None:
            self.data['totais'] = {
                'base_calculo_icms': self._safe_float(total_node.findtext('nfe:vBC', None, self.ns)),
                'valor_icms': self._safe_float(total_node.findtext('nfe:vICMS', None, self.ns)),
                'base_calculo_icms_st': self._safe_float(total_node.findtext('nfe:vBCST', None, self.ns)),
                'valor_icms_st': self._safe_float(total_node.findtext('nfe:vST', None, self.ns)),
                'valor_produtos': self._safe_float(total_node.findtext('nfe:vProd', None, self.ns)),
                'valor_frete': self._safe_float(total_node.findtext('nfe:vFrete', None, self.ns)),
                'valor_seguro': self._safe_float(total_node.findtext('nfe:vSeg', None, self.ns)),
                'valor_desconto': self._safe_float(total_node.findtext('nfe:vDesc', None, self.ns)),
                'valor_ii': self._safe_float(total_node.findtext('nfe:vII', None, self.ns)),
                'valor_ipi': self._safe_float(total_node.findtext('nfe:vIPI', None, self.ns)),
                'valor_pis': self._safe_float(total_node.findtext('nfe:vPIS', None, self.ns)),
                'valor_cofins': self._safe_float(total_node.findtext('nfe:vCOFINS', None, self.ns)),
                'valor_outros': self._safe_float(total_node.findtext('nfe:vOutro', None, self.ns)),
                'valor_total_nfe': self._safe_float(total_node.findtext('nfe:vNF', None, self.ns)),
            }
        
        return self

    def set_transporte(self, transp):
        self.data['transporte']['modalidade_frete'] = self._safe_int(transp.findtext('nfe:modFrete', '9', self.ns))
        return self

    def set_transportadora(self, transporta):
        if transporta is not None:
            self.data['transportadora'] = {
                'tipo_pessoa': 'TRANSPORTADORA',
                'cnpj': transporta.findtext('nfe:CNPJ', None, self.ns),
                'cpf': transporta.findtext('nfe:CPF', None, self.ns),
                'nome': transporta.findtext('nfe:xNome', '', self.ns),
                'inscricao_estadual': transporta.findtext('nfe:IE', None, self.ns),
            }
        return self

    def set_transportadora_endereco(self, transporta):
        if transporta is not None:
            self.data['transportadora_endereco'] = {
                'tipo_endereco': 'PRINCIPAL',
                'logradouro': transporta.findtext('nfe:xEnder', 'N/A', self.ns),
                'numero': 'S/N',
                'bairro': 'N/A',
                'codigo_municipio': '0000000',
                'municipio': transporta.findtext('nfe:xMun', 'N/A', self.ns),
                'uf': transporta.findtext('nfe:UF', 'NA', self.ns),
                'cep': '00000000'
            }
        return self

    def add_volume(self, vols):
        for vol in vols:
            self.data['volumes'].append({
                'tipo_item': 'VOLUME',
                'quantidade': self._safe_int(vol.findtext('nfe:qVol', None, self.ns)),
                'especie': vol.findtext('nfe:esp', None, self.ns),
                'marca': vol.findtext('nfe:marca', None, self.ns),
                'numeracao': vol.findtext('nfe:nVol', None, self.ns),
                'peso_liquido': self._safe_float(vol.findtext('nfe:pesoL', None, self.ns)),
                'peso_bruto': self._safe_float(vol.findtext('nfe:pesoB', None, self.ns)),
            })
        return self

    def set_informacoes_adicionais(self, infAdic):
        if infAdic is not None:
            self.data['informacoes_adicionais'] = {
                'info_contribuinte': infAdic.findtext('nfe:infCpl', None, self.ns),
                'info_fisco': infAdic.findtext('nfe:infAdFisco', None, self.ns)
            }

        return self