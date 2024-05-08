class Trade:
    entry_id_counter = 1

    def __init__(self, entry_signal, entry_datetime, entry_price, max_exits):
        self.entry_id = Trade.entry_id_counter
        Trade.entry_id_counter += 1

        self.entry_signal = entry_signal
        self.entry_datetime = entry_datetime
        self.entry_price = entry_price
        self.exits = []
        self.trade_closed = False
        self.max_exits = max_exits
        self.exit_id = 1

    def calculate_pnl(self, exit_price):
        pnl = 0
        if self.entry_signal == 'long':
            pnl = exit_price - self.entry_price
        else:
            pnl = self.entry_price - exit_price
        return pnl

    def add_exit(self, exit_datetime, exit_price, exit_type):
        if not self.trade_closed:
            self.exits.append(
                {
                    'exit_id': self.exit_id,
                    'exit_datetime': exit_datetime,
                    'exit_price': exit_price,
                    'exit_type': exit_type,
                    'pnl': self.calculate_pnl(exit_price)
                }
            )
            self.exit_id += 1

            # Check for trade closure based on max_exits and exit_type
            if ((self.max_exits != 'ALL' and len(self.exits) >= self.max_exits)
                    or exit_type == 'tag_change'):
                self.trade_closed = True

    def is_trade_closed(self):
        return self.trade_closed

    def to_dict(self):
        return {
            'entry_id': self.entry_id,
            'entry_signal': self.entry_signal,
            'entry_datetime': self.entry_datetime,
            'entry_price': self.entry_price,
            'exits': self.exits,
            'trade_closed': self.trade_closed
        }
