module testdataframe.io;

import typeddataframe;

import std.stdio;

void testIo() {
   auto df = readCsv!(ulong, float, float, float, float, float)("iris.csv");
   writeln(df);
}
