import dataframe;

import std.stdio;

import numir;
import mir.ndslice;

void main()
{
   auto df = new TypedDataFrame!(float, double, string)("DataFrame DF");
   df.setcol(0, "a", zeros!float(10));
   df.setcol(1, "b", ones!double(10));
   df.setcol(2, "c", ["a", "b", "c", "d", "e", "f", "g", "h", "i", "j"].sliced(10));

   auto col0 = df.col!0();
   writeln(col0);

   auto col1 = df.col!double(1);
   writeln(col1);

   auto col11 = df.col!1();
   writeln(col11);

   writeln(df);

   auto x = zeros!float(10);
   x[0] = 1.0f;
   foreach(i; 1..10) {
      x[i] = x[i-1] + 1.0f;
   }

   {
      writeln("Add column at the end");
      auto newdf = df.addcol!float("d", x);
      newdf.setTitle("DataFrame NEW DF");
      writeln(newdf);
   }

   {
      writeln("Add column at the begining");
      auto newdf = df.addcol!(float, 0)("d", x);
      writeln(newdf);
   }

   {
      writeln("Add column in the middle");
      auto newdf = df.addcol!(float, 1)("d", x);
      writeln(newdf);
   }

   {
      auto newdf = new DataFrame!(float, float)();
      newdf.setTitle("DataFrame using DataFrame");
      newdf.setcol(0, "a", x);
      newdf.setcol(1, "b", x);
      writeln(newdf);
   }

}
