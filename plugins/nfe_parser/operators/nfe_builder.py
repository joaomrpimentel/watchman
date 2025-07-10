from .product_nfe_builder import Product_NfeBuilder


class Builder:
    def __init__(self):
        return None

    def build_ProductNfe(self, root):
        builder = Product_NfeBuilder()

        self.log.info("Endereço da transportadora não é estruturado. Usando placeholders.")

        infNFe = root.find('.//nfe:infNFe', self.ns)
        ide = root.find('.//nfe:ide', self.ns)
        emit = root.find('.//nfe:emit', self.ns)
        enderEmit = emit.find('nfe:enderEmit', self.ns)
        dest = root.find('.//nfe:dest', self.ns)
        enderDest = dest.find('nfe:enderDest', self.ns)
        items = root.findall('.//nfe:det', self.ns)
        total_node = root.find('.//nfe:ICMSTot', self.ns)
        transp = root.find('.//nfe:transp', self.ns)
        transporta = transp.find('nfe:transporta', self.ns)
        vols = transp.findall('.//nfe:vol', self.ns)
        infAdic = root.find('.//nfe:infAdic', self.ns)

        builder.set_nfe(infNFe, ide)
        builder.set_emitente(emit)
        builder.set_emitente_endereco(enderEmit)
        builder.set_destinatario(dest)
        builder.set_destinatario_endereco(enderDest)
        builder.add_items(items)
        builder.set_totais(total_node)
        builder.set_transporte(transp)
        builder.set_transportadora(transporta)
        builder.set_transportadora_endereco(transporta)
        builder.add_volume(vols)
        builder.set_informacoes_adicionais(infAdic)

    def build_ServiceNfe(self, root):
        self.log.info("Ainda não tem suporte para notas fiscais de serviço.")