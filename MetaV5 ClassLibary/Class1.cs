using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.Json;

namespace MetaDB
{
    public enum FieldType { String, Number }

    public class MetaDatabase
    {
        public Dictionary<string, Table> Tables { get; private set; } = new();

        public void Open(string file)
        {
            if (string.IsNullOrWhiteSpace(file)) throw new ArgumentException("Filename cannot be empty.");

            if (File.Exists(file))
                Tables = JsonSerializer.Deserialize<Dictionary<string, Table>>(File.ReadAllText(file));
            else
                Save(file);
        }

        public void Save(string file)
        {
            if (string.IsNullOrEmpty(file)) throw new InvalidOperationException("No database file specified.");
            File.WriteAllText(file, JsonSerializer.Serialize(Tables, new JsonSerializerOptions { WriteIndented = true }));
        }

        public void CreateTable(string tableName)
        {
            if (Tables.ContainsKey(tableName)) throw new InvalidOperationException("Table already exists.");
            Tables[tableName] = new Table(tableName);
        }

        public void Push(string tableName, Dictionary<string, object> record)
        {
            tableName = MatchTableName(tableName);
            Tables[tableName].AddRecord(record);
        }

        public IEnumerable<Dictionary<string, object>> Pull(string tableName, string condition = "")
        {
            tableName = MatchTableName(tableName);
            return Tables[tableName].Query(condition);
        }

        public int Set(string tableName, string condition, string key, object value)
        {
            tableName = MatchTableName(tableName);
            return Tables[tableName].Update(condition, key, value);
        }

        public int Delete(string tableName, string condition)
        {
            tableName = MatchTableName(tableName);
            return Tables[tableName].Delete(condition);
        }

        private string MatchTableName(string input)
        {
            if (Tables.ContainsKey(input)) return input;
            var match = Tables.Keys.FirstOrDefault(k => string.Equals(k, input, StringComparison.OrdinalIgnoreCase));
            if (match != null)
            {
                Console.WriteLine($"Did you mean {match}? If so, use: {match}");
                return match;
            }
            throw new InvalidOperationException($"Table '{input}' not found.");
        }
    }

    public class Table
    {
        public string Name { get; private set; }
        public List<Dictionary<string, object>> Rows { get; private set; } = new();
        public Dictionary<string, FieldType> FieldTypes { get; private set; } = new();

        private Dictionary<string, SortedDictionary<double, List<int>>> NumericIndexes = new();
        private Dictionary<string, Dictionary<string, List<int>>> StringIndexes = new();

        public Table(string name) { Name = name; }

        public void AddRecord(Dictionary<string, object> record)
        {
            int index = Rows.Count;
            Rows.Add(record);

            foreach (var kv in record)
            {
                if (!FieldTypes.ContainsKey(kv.Key))
                {
                    FieldTypes[kv.Key] = kv.Value is double or int ? FieldType.Number : FieldType.String;
                }

                if (FieldTypes[kv.Key] == FieldType.Number)
                {
                    if (!NumericIndexes.ContainsKey(kv.Key)) NumericIndexes[kv.Key] = new SortedDictionary<double, List<int>>();
                    double num = Convert.ToDouble(kv.Value);
                    if (!NumericIndexes[kv.Key].ContainsKey(num)) NumericIndexes[kv.Key][num] = new List<int>();
                    NumericIndexes[kv.Key][num].Add(index);
                }
                else
                {
                    if (!StringIndexes.ContainsKey(kv.Key)) StringIndexes[kv.Key] = new Dictionary<string, List<int>>();
                    string str = kv.Value.ToString();
                    if (!StringIndexes[kv.Key].ContainsKey(str)) StringIndexes[kv.Key][str] = new List<int>();
                    StringIndexes[kv.Key][str].Add(index);
                }
            }
        }

        public IEnumerable<Dictionary<string, object>> Query(string condition)
        {
            for (int i = 0; i < Rows.Count; i++)
            {
                if (Matches(Rows[i], condition)) yield return Rows[i];
            }
        }

        public int Update(string condition, string key, object value)
        {
            int count = 0;
            for (int i = 0; i < Rows.Count; i++)
            {
                if (Matches(Rows[i], condition))
                {
                    Rows[i][key] = value;
                    count++;
                }
            }
            return count;
        }

        public int Delete(string condition)
        {
            int before = Rows.Count;
            Rows = Rows.Where(r => !Matches(r, condition)).ToList();
            return before - Rows.Count;
        }

        private bool Matches(Dictionary<string, object> record, string condition)
        {
            if (string.IsNullOrWhiteSpace(condition)) return true;
            string[] ops = { "==", ">=", "<=", ">", "<" };
            foreach (var op in ops)
            {
                if (condition.Contains(op))
                {
                    var parts = condition.Split(op);
                    string key = parts[0].Trim();
                    string val = parts[1].Trim().Trim('"');

                    if (!record.ContainsKey(key)) return false;

                    if (FieldTypes[key] == FieldType.Number)
                    {
                        double rv1 = Convert.ToDouble(record[key]);
                        double rv2 = double.Parse(val);
                        return op switch
                        {
                            "==" => rv1 == rv2,
                            ">=" => rv1 >= rv2,
                            "<=" => rv1 <= rv2,
                            ">" => rv1 > rv2,
                            "<" => rv1 < rv2,
                            _ => false
                        };
                    }
                    else
                    {
                        string rv1 = record[key].ToString();
                        return op switch
                        {
                            "==" => rv1 == val,
                            "!=" => rv1 != val,
                            _ => false
                        };
                    }
                }
            }
            return false;
        }
    }
}
