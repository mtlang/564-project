-- description: close auctions when bid at buy price

PRAGMA foreign_keys = ON;

drop trigger if exists trigger9;

create trigger trigger9
	after insert on Bids
	for each row when (New.Amount >= (Select i.Buy_Price from Items i where NEW.ItemID = i.ItemID))
	begin
		UPDATE Items SET Ends = (Select c.Time from CurrentTime c) WHERE New.ItemID = Items.ItemID;
	end;