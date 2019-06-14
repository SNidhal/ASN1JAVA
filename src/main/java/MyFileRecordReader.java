import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;
import org.bouncycastle.asn1.ASN1InputStream;
import org.bouncycastle.asn1.ASN1Primitive;
import org.bouncycastle.asn1.ASN1Sequence;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;

public class MyFileRecordReader extends RecordReader<LongWritable ,Text >  {
    private Path path;
    private InputStream is;
    private FSDataInputStream fsin;
    private ASN1InputStream asnin;
    private ASN1Primitive obj;

    private long start,end,postition=0;
    private LineReader in;

    private  LongWritable currentKey=new LongWritable();
    private Text currentValue=new Text();
    private boolean isProcessed = false;


    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        System.out.println("next key position: "+postition);
        currentKey.set(postition);
        currentValue.clear();
        int newSize=0;

        System.out.println("next key fsin: "+fsin.getPos());
        System.out.println("pos < end"+(postition<end)+"first :"+fsin.getPos());

        while(postition<end){
            System.out.println("while");
            //fsin.seek(postition);
            int cmp=0;
            int i;
            long taille =Integer.MAX_VALUE;
            fsin.seek(postition);
            //System.out.println("fsin read :"+(fsin.read()!=50 )+"//// cmp + pos :"+ (cmp+postition>end-1));
            while( (i=fsin.readByte())!=-1 && fsin.getPos()<end){
                cmp++;
                if (cmp==2){
                    taille=i+fsin.getPos();
                }
                if(fsin.getPos()<=taille) {
                    //if(postition==208) System.out.println("156 ahaw");
                    //newSize++;
                    System.out.println("byte: " + i+ "----------- pos  :"+fsin.getPos()+"----------taille   :"+taille);
                    byte[] b = {(byte) i};
                    currentValue.append(b, 0, 1);
                }else {
                    System.out.println(i);
                    postition=taille;
                    return true;
                }
            }
            byte[] b = {(byte) i};
            currentValue.append(b, 0, 1);
            newSize= (int) taille;
          if(newSize==0){
                break;
            }
            postition=taille;
           // System.out.println("new pos: "+postition+"+++++++++++ taille : "+taille);
        }
        if(newSize==0){
            currentKey=null;
            currentValue=null;
            return false;

        }else {
            return true;
        }
     /*   if (isProcessed) return false;



        int recordCounter = 0;
        while ((obj = asnin.readObject()) != null) {

            CallDetailRecord thisCdr = new CallDetailRecord((ASN1Sequence) obj);
            recordCounter++;

            System.out.println("CallDetailRecord "+thisCdr.getRecordNumber()+" Calling "+thisCdr.getCallingNumber()
                    +" Called "+thisCdr.getCalledNumber()+ " Start Date-Time "+thisCdr.getStartDate()+"-"
                    +thisCdr.getStartTime()+" duration "+thisCdr.getDuration()
            );

        }
        isProcessed = true;
        currentKey = new LongWritable( recordCounter);
        //Return number of records
        currentValue = new Text(String.valueOf(recordCounter));

        return true;*/
    }

    @Override
    public LongWritable getCurrentKey() throws IOException, InterruptedException {
        return currentKey;
    }

    @Override
    public  Text getCurrentValue() throws IOException, InterruptedException {
        return currentValue;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        if(start==end){
            return 0.0f;
        }else{
            return Math.min(1.0f,(postition-start)/(float)(end-start));
        }
    }

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context)
            throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        /////////////////
        FileSplit isplit =(FileSplit) split;
        //start=Integer.max((int) (isplit.getStart()-1),0);
        start=isplit.getStart();
        end =start+isplit.getLength();

        /////////////////////////
        System.out.println("start:"+start+"************ end: "+end);
        //////////////////////////
        path = ((FileSplit) split).getPath();
        FileSystem fs = path.getFileSystem(conf);
        fsin = fs.open(path);
        //System.out.println("pos :     ---------"+fsin.readByte());
        //tessssssssssssssst
        boolean skipFirstRecord = false;
        if(start!=0){
            skipFirstRecord=true;
            --start;
            fsin.seek(start);
        }
        byte[] bt ={50};
        //in =new LineReader(fsin,conf,bt);

        if(skipFirstRecord){
            Text dummy = new Text();
            int btSize=0;
            while(fsin.read()!=50){
                btSize++;
                //System.out.println("Skip first recored"+fsin.read());
            }
            start+=btSize;
        }
        System.out.println("Dont Skip first recored");

    postition=start;
        ////////////////////////////
     /*   fsin.seek(start);
        int firstByte=fsin.readByte();
        if(firstByte==50)start=Integer.max((int) (start-1),0);
        fsin.seek(start);

        if(start!=0)


        while(fsin.read()!=-1)System.out.println(fsin.read());
        is=decompressStream(fsin);

        asnin = new ASN1InputStream(is);*/
    }

    @Override
    public void close() throws IOException {
       // asnin.close();
        //is.close();
        if (fsin!=null) fsin.close();
    }

    public static InputStream decompressStream(InputStream input) {
        InputStream returnStream=null;
        org.apache.commons.compress.compressors.CompressorInputStream cis = null;
        BufferedInputStream bis=null;
        try {
            bis = new BufferedInputStream(input);
            bis.mark(1024);   //Mark stream to reset if uncompressed data
            cis = new org.apache.commons.compress.compressors.CompressorStreamFactory().createCompressorInputStream(bis);
            returnStream = cis;
        } catch (org.apache.commons.compress.compressors.CompressorException ce) { //CompressorStreamFactory throws CompressorException for uncompressed files
            try {
                bis.reset();
            } catch (IOException ioe) {
                String errmessageIOE="IO Exception ( "+ioe.getClass().getName()+" ) : "+ioe.getMessage();
                System.out.println(errmessageIOE);
            }
            returnStream = bis;
        } catch (Exception e) {
            String errmessage="Exception ( "+e.getClass().getName()+" ) : "+e.getMessage();
            System.out.println(errmessage);
        }
        return returnStream;
    }
}
