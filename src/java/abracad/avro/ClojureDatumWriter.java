package abracad.avro;

import java.io.IOException;

import org.apache.avro.Conversion;
import org.apache.avro.LogicalType;
import org.apache.avro.Schema;
import org.apache.avro.UnresolvedUnionException;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.Encoder;

import clojure.lang.RT;
import clojure.lang.Symbol;
import clojure.lang.Var;

public class ClojureDatumWriter extends GenericDatumWriter<Object> {

private static class Vars {
    private static final String NS = "abracad.avro.write";

    private static final Var writeRecord = RT.var(NS, "write-record");
    private static final Var writeEnum = RT.var(NS, "write-enum");
    private static final Var writeArray = RT.var(NS, "write-array");
    private static final Var writeBytes = RT.var(NS, "write-bytes");

    static {
        RT.var("clojure.core", "require").invoke(Symbol.intern(NS));
    }
}

public
ClojureDatumWriter() {
    super();
}

public
ClojureDatumWriter(Schema schema) {
    super(schema);
}

@Override
public void
write(Schema schema, Object datum, Encoder out) throws IOException {
    try {
        LogicalType logicalType = schema.getLogicalType();
        if (datum != null && logicalType != null) {
            Conversion<?> conversion = getData().getConversionByClass(datum.getClass(), logicalType);
            writeWithoutConversion(schema, convert(schema, logicalType, conversion, datum), out);
        }
        else {
            switch (schema.getType()) {
            case INT: out.writeInt(RT.intCast(datum)); break;
            case LONG: out.writeLong(RT.longCast(datum)); break;
            case FLOAT: out.writeFloat(RT.floatCast(datum)); break;
            case DOUBLE: out.writeDouble(RT.doubleCast(datum)); break;
            case BOOLEAN: out.writeBoolean(RT.booleanCast(datum)); break;
            default: super.write(schema, datum, out); break;
            }
        }
    } catch (NullPointerException e) {
        throw super.npe(e, " of " + schema.getFullName());
    }
}

@Override
protected void
writeRecord(Schema schema, Object datum, Encoder out)
        throws IOException {
    Vars.writeRecord.invoke(this, schema, datum, out);
}

@Override
protected void
writeEnum(Schema schema, Object datum, Encoder out)
        throws IOException {
    Vars.writeEnum.invoke(this, schema, datum, out);
}

@Override
protected void
writeArray(Schema schema, Object datum, Encoder out)
        throws IOException {
    Vars.writeArray.invoke(this, schema, datum, out);
}

@Override
protected void
writeBytes(Object datum, Encoder out)
        throws IOException {
    Vars.writeBytes.invoke(this, datum, out);
}

}
