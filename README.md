# typeddataframe

Data frames with type information.

## Getting started

```d
import typeddataframe;
import numir;

void main()
{
   auto df = dataFrame!(float, double, string)(
      "x", empty!float(3), "y", empty!double(3), "z", ["A", "B", "C"].sliced(3));
   // shape
   writeln(df.shape());
   // print
   writeln(df);
   // Get the column at index
   writeln(df[0]);
   // Get the column from its name
   writeln(df["z"]);
   // Slicing
   auto slice = df[0..3, 0];
   // Assign a value
   slice = 1.0f;
   writeln(df);
   // Assign a mir slice
   slice = [1.0f, 2.0f, 3.0f].sliced(3);
   writeln(df);
}
```

