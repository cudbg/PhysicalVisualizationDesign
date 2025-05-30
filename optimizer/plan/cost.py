class Memory:
    """
    A memory information contains two parts: server memory and client memory
    """

    def __init__(self, server: float, client: float):
        self.server = server
        self.client = client

    def satisfy(self, other: 'Memory') -> bool:
        return self.server <= other.server and self.client <= other.client

    def __add__(self, other: 'Memory') -> 'Memory':
        return Memory(self.server + other.server, self.client + other.client)

    def __sub__(self, other: 'Memory') -> 'Memory':
        return Memory(self.server - other.server, self.client - other.client)

    def __str__(self) -> str:
        return f"MEM[S({self.server})C({self.client})]"


class Latency:
    """
    A latency information contains two parts: query latency and switch on latency
    """

    def __init__(self, latency: float, switch_on_latency: float):
        self.latency = latency
        self.switch_on_latency = switch_on_latency

    def satisfy(self, other: 'Latency') -> bool:
        return self.latency <= other.latency and self.switch_on_latency <= other.switch_on_latency

    def __add__(self, other: 'Latency') -> 'Latency':
        return Latency(self.latency + other.latency, self.switch_on_latency + other.switch_on_latency)

    def __sub__(self, other: 'Latency') -> 'Latency':
        return Latency(self.latency - other.latency, self.switch_on_latency - other.switch_on_latency)

    def __str__(self) -> str:
        return f"LAT[L({self.latency})SO({self.switch_on_latency})]"


class Cost:
    """
    A cost contains two parts: latency and memory
    """

    def __init__(self, avg_latency: Latency, upper_latency: Latency, mem: Memory) -> None:
        self.avg_latency = avg_latency
        self.upper_latency = upper_latency
        self.memory = mem

    def __add__(self, other: 'Cost') -> 'Cost':
        return Cost(self.avg_latency + other.avg_latency,
                    self.upper_latency + other.upper_latency,
                    self.memory + other.memory)

    def satisfy(self, other: 'Cost', target=("upper", "mem")) -> bool:
        for t in target:
            if t == "avg" and not self.avg_latency.satisfy(other.avg_latency):
                return False
            if t == "upper" and not self.upper_latency.satisfy(other.upper_latency):
                return False
            elif t == "mem" and not self.memory.satisfy(other.memory):
                return False
        return True

    def __str__(self) -> str:
        return f"COST[AVG={self.avg_latency};UPPER={self.upper_latency};{self.memory})]"

class Selectivity:
    def __init__(self, avg_sel: float, upper_sel: float) -> None:
        self.avg_sel = avg_sel
        self.upper_sel = upper_sel
    def __str__(self) -> str:
        return f"SEL[avg={self.avg_sel}; upper={self.upper_sel}]"

latency_coef = \
{('Aggregate', 0): {'bias': 0,
                    'input_num_cols': 3.5808894132616493e-09,
                    'input_num_rows': 3.688316656897741e-06,
                    'input_num_string_cols': 1.7874761674147366e-10,
                    'input_size': 0.0,
                    'input_string_size': 9.211850347611787e-06,
                    'output_num_cols': 2.3234835989765628e-09,
                    'output_num_rows': 8.94427544472402e-05,
                    'output_num_string_cols': 1.640661253578013e-10,
                    'output_size': 2.460235339861579e-06,
                    'output_string_size': 4.116772229164864e-05},
 ('Aggregate', 1): {'bias': 0,
                    'input_num_cols': 1.2543886315543277e-10,
                    'input_num_rows': 3.2861936168964418e-06,
                    'input_num_string_cols': 0.0,
                    'input_size': 4.849006038675298e-07,
                    'input_string_size': 6.477754042724001e-06,
                    'output_num_cols': 2.7456594939516845e-10,
                    'output_num_rows': 6.19019170032429e-05,
                    'output_num_string_cols': 9.798674489551537e-11,
                    'output_size': 4.4813247861109364e-06,
                    'output_string_size': 6.330905615560374e-05},
 ('Filter', 0): {'bias': 0,
                 'input_num_cols': 0.0,
                 'input_num_rows': 3.688316656897741e-06,
                 'input_num_string_cols': 0.0,
                 'input_size': 0.0,
                 'input_string_size': 5.152727262303094e-06,
                 'output_num_cols': 0.0,
                 'output_num_rows': 1.9466553595238176e-05,
                 'output_num_string_cols': 0.0,
                 'output_size': 0.0,
                 'output_string_size': 1.0297999486357926e-06},
 ('Filter', 1): {'bias': 0,
                 'input_num_cols': 0.0,
                 'input_num_rows': 3.2861936168964418e-06,
                 'input_num_string_cols': 0.0,
                 'input_size': 4.849006038675298e-07,
                 'input_string_size': 0.0,
                 'output_num_cols': 0.0,
                 'output_num_rows': 6.356773337885631e-06,
                 'output_num_string_cols': 0.0,
                 'output_size': 4.849006038675298e-07,
                 'output_string_size': 1.8340309698944553e-05},
 ('HashTableBuild', 0): {'bias': 0,
                         'input_num_cols': 0.0,
                         'input_num_rows': 0.0,
                         'input_num_string_cols': 0.0,
                         'input_size': 5.414070096589101e-05,
                         'input_string_size': 0.0001932438739549069,
                         'output_num_cols': 0.0,
                         'output_num_rows': 0.0,
                         'output_num_string_cols': 0.0,
                         'output_size': 5.414070096589101e-05,
                         'output_string_size': 0.0001932438739549069},
 ('HashTableBuild', 1): {'bias': 0,
                         'input_num_cols': 0.0,
                         'input_num_rows': 2.264611384516987e-05,
                         'input_num_string_cols': 0.0,
                         'input_size': 0.0,
                         'input_string_size': 0.00019339133256038283,
                         'output_num_cols': 0.0,
                         'output_num_rows': 2.264611384516987e-05,
                         'output_num_string_cols': 0.0,
                         'output_size': 0.0,
                         'output_string_size': 0.00019339133256038283},
 ('PrefixSum2DBuild', 0): {'bias': 0,
                           'input_num_cols': 5.057106524320816e-05,
                           'input_num_rows': 2.339741941970365e-07,
                           'input_num_string_cols': 0.0,
                           'input_size': 1.6379761525066798e-06,
                           'input_string_size': 0.0,
                           'output_num_cols': 0.007422588308185941,
                           'output_num_rows': 0.00015611517643248645,
                           'output_num_string_cols': 0.0,
                           'output_size': 0.0,
                           'output_string_size': 0.0},
 ('PrefixSum2DBuild', 1): {'bias': 0,
                           'input_num_cols': 1.6968527912771269e-10,
                           'input_num_rows': 3.892861048628584e-07,
                           'input_num_string_cols': 0.0,
                           'input_size': 2.725002734040026e-06,
                           'input_string_size': 0.0,
                           'output_num_cols': 2.1207487333735027e-08,
                           'output_num_rows': 5.574563377977135e-10,
                           'output_num_string_cols': 0.0,
                           'output_size': 0.0,
                           'output_string_size': 0.0},
 ('PrefixSumBuild', 0): {'bias': 0,
                         'input_num_cols': 5.25031451977622e-06,
                         'input_num_rows': 1.7922477653542914e-05,
                         'input_num_string_cols': 4.2734522348194857e-07,
                         'input_size': 0.0,
                         'input_string_size': 0.0006546575298644826,
                         'output_num_cols': 0.0004872705010435378,
                         'output_num_rows': 2.9064429209640917e-05,
                         'output_num_string_cols': 3.164150133634569e-07,
                         'output_size': 0.0,
                         'output_string_size': 2.6612865091674008e-05},
 ('PrefixSumBuild', 1): {'bias': 0,
                         'input_num_cols': 5.519529854577486e-08,
                         'input_num_rows': 3.3939822123887728e-06,
                         'input_num_string_cols': 1.668790201470601e-08,
                         'input_size': 2.6442236968769525e-07,
                         'input_string_size': 2.3494507805555217e-05,
                         'output_num_cols': 2.0490390854611845e-05,
                         'output_num_rows': 6.664509022787581e-07,
                         'output_num_string_cols': 1.105590463514249e-08,
                         'output_size': 1.905473388545717e-07,
                         'output_string_size': 8.946039886530704e-07},
 ('RTreeBuild', 0): {'bias': 0,
                     'input_num_cols': 0.0,
                     'input_num_rows': 0.0005653098614319326,
                     'input_num_string_cols': 0.0,
                     'input_size': 0.0,
                     'input_string_size': 0.0,
                     'output_num_cols': 0.0,
                     'output_num_rows': 0.0005653098614319326,
                     'output_num_string_cols': 0.0,
                     'output_size': 0.0,
                     'output_string_size': 0.0},
 ('RTreeBuild', 1): {'bias': 0,
                     'input_num_cols': 0.0,
                     'input_num_rows': 0.0007176082114239469,
                     'input_num_string_cols': 0.0,
                     'input_size': 0.0,
                     'input_string_size': 0.0,
                     'output_num_cols': 0.0,
                     'output_num_rows': 0.0007176082114239469,
                     'output_num_string_cols': 0.0,
                     'output_size': 0.0,
                     'output_string_size': 0.0},
 ('RTreeQuery', 0): {'bias': 0,
                     'input_num_cols': 3.12138031405307e-09,
                     'input_num_rows': 0.0,
                     'input_num_string_cols': 0.0,
                     'input_size': 0.0,
                     'input_string_size': 0.0,
                     'output_num_cols': 3.12138031405307e-09,
                     'output_num_rows': 0.0008985101671848668,
                     'output_num_string_cols': 0.0,
                     'output_size': 0.0,
                     'output_string_size': 0.0},
 ('RTreeQuery', 1): {'bias': 0,
                     'input_num_cols': 2.142976034915312e-08,
                     'input_num_rows': 1.8278055925581062e-09,
                     'input_num_string_cols': 0.0,
                     'input_size': 2.1857269492020548e-09,
                     'input_string_size': 0.0,
                     'output_num_cols': 2.142976034915312e-08,
                     'output_num_rows': 0.0006194437859319403,
                     'output_num_string_cols': 0.0,
                     'output_size': 2.1857269492020548e-09,
                     'output_string_size': 0.0}}

memory_coef = \
{'HashTableBuild': {'bias': 0,
                    'output_num_cols': 8.580710282392009e-05,
                    'output_num_rows': 0.0,
                    'output_num_string_cols': 1.6334008042024962e-05,
                    'output_size': 12.58383515608283,
                    'output_string_size': 0.0},
 'PrefixSum2DBuild': {'bias': 0,
                      'output_num_cols': 45.99520594627741,
                      'output_num_rows': 5577.898341422516,
                      'output_num_string_cols': 0.0,
                      'output_size': 5.094270631899839e-05,
                      'output_string_size': 0.0},
 'PrefixSumBuild': {'bias': 0,
                    'output_num_cols': 408.0407117314361,
                    'output_num_rows': 0.0,
                    'output_num_string_cols': 0.0,
                    'output_size': 0.0016481451409082087,
                    'output_string_size': 4955.970055768963},
 'RTreeBuild': {'bias': 0,
                'output_num_cols': 0.0,
                'output_num_rows': 61.49557390807676,
                'output_num_string_cols': 0.0,
                'output_size': 6.921725754237268,
                'output_string_size': 0.0},
 'Table': {'bias': 0,
           'output_num_cols': 0.0,
           'output_num_rows': 0.0,
           'output_num_string_cols': 2.1901878425442843e-06,
           'output_size': 6.319028272613862,
           'output_string_size': 17.311274971046945}}
