package com.netflix.jmeter.gui;

import java.awt.GridBagConstraints;

import javax.swing.JCheckBox;
import javax.swing.JComboBox;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JTextField;

import org.apache.jmeter.testelement.TestElement;

import com.netflix.jmeter.gui.components.CompositeSerializerList;
import com.netflix.jmeter.sampler.EnhancedAbstractSampler;
import com.netflix.jmeter.sampler.EnhancedGetRangeSliceSampler;
import com.netflix.jmeter.sampler.EnhancedGetIndexRangeSliceSampler;


public class EnhancedGetIndexRangeSlice extends EnhancedAbstractGUI
{	
    private static final long serialVersionUID = 3197090412869386190L;
    private static final String LABEL = "Cassandra Get Index Range Slice";

    private JTextField INDEX_COLUMN_NAME;
    private JTextField INDEX_COLUMN_VALUE;
    
	private JTextField START_COLUMN_NAME;
    private JTextField END_COLUMN_NAME;
    private JTextField COUNT;
    private JCheckBox IS_REVERSE;

    private JComboBox KSERIALIZER;
    private JComboBox CSERIALIZER;
    private JComboBox VSERIALIZER;
    
    private CompositeSerializerList COMPOSITE_KSERIALIZERS_LIST;
    private CompositeSerializerList COMPOSITE_CSERIALIZERS_LIST;
    private CompositeSerializerList COMPOSITE_VSERIALIZERS_LIST;
    
   
    @Override
    public void configure(TestElement element)
    {
        super.configure(element);
        
        INDEX_COLUMN_NAME.setText(element.getPropertyAsString(EnhancedGetIndexRangeSliceSampler.INDEX_COLUMN_NAME));
        INDEX_COLUMN_VALUE.setText(element.getPropertyAsString(EnhancedGetIndexRangeSliceSampler.INDEX_COLUMN_VALUE));
        
        START_COLUMN_NAME.setText(element.getPropertyAsString(EnhancedGetIndexRangeSliceSampler.START_COLUMN_NAME));
        END_COLUMN_NAME.setText(element.getPropertyAsString(EnhancedGetIndexRangeSliceSampler.END_COLUMN_NAME));
        COUNT.setText(element.getPropertyAsString(EnhancedGetIndexRangeSliceSampler.COUNT));
        IS_REVERSE.setSelected(element.getPropertyAsBoolean(EnhancedGetIndexRangeSliceSampler.IS_REVERSE));

        KSERIALIZER.setSelectedItem(element.getPropertyAsString(EnhancedGetIndexRangeSliceSampler.KEY_SERIALIZER_TYPE));
        CSERIALIZER.setSelectedItem(element.getPropertyAsString(EnhancedGetIndexRangeSliceSampler.COLUMN_SERIALIZER_TYPE));
        VSERIALIZER.setSelectedItem(element.getPropertyAsString(EnhancedGetIndexRangeSliceSampler.VALUE_SERIALIZER_TYPE));
        COMPOSITE_KSERIALIZERS_LIST.setListModelElements(EnhancedGetRangeSliceSampler.getAsArray(element.getPropertyAsString(EnhancedGetRangeSliceSampler.COMPOSITE_KEY_SERIALIZER_TYPES), ","));
        COMPOSITE_CSERIALIZERS_LIST.setListModelElements(EnhancedGetRangeSliceSampler.getAsArray(element.getPropertyAsString(EnhancedGetRangeSliceSampler.COMPOSITE_COLUMN_SERIALIZER_TYPES), ","));
        COMPOSITE_VSERIALIZERS_LIST.setListModelElements(EnhancedGetRangeSliceSampler.getAsArray(element.getPropertyAsString(EnhancedGetRangeSliceSampler.COMPOSITE_VALUE_SERIALIZER_TYPES), ","));       
    }
    
    public TestElement createTestElement()
    {
    	EnhancedGetIndexRangeSliceSampler sampler = new EnhancedGetIndexRangeSliceSampler();
        modifyTestElement(sampler);
        sampler.setComment("test comment");     
        return sampler;
        
    }

    public void modifyTestElement(TestElement sampler)
    {
        super.configureTestElement(sampler);

        if (sampler instanceof EnhancedGetIndexRangeSliceSampler)
        {
        	EnhancedGetIndexRangeSliceSampler gSampler = (EnhancedGetIndexRangeSliceSampler) sampler;
        	gSampler.setKSerializerType((String) KSERIALIZER.getSelectedItem());
            gSampler.setCSerializerType((String) CSERIALIZER.getSelectedItem());
            gSampler.setVSerializerType((String) VSERIALIZER.getSelectedItem());

            gSampler.setIndexName(INDEX_COLUMN_NAME.getText());
            gSampler.setIndexValue(INDEX_COLUMN_VALUE.getText());

            gSampler.setStartName(START_COLUMN_NAME.getText());
            gSampler.setEndName(END_COLUMN_NAME.getText());
            gSampler.setCount(COUNT.getText());
            gSampler.setReverse(IS_REVERSE.isSelected());
            
            if("CompositeSerializer".equals(gSampler.getKSerializerType())){
            	gSampler.setCompositeKSerializerTypes(COMPOSITE_KSERIALIZERS_LIST.getListModelAsArray());
            }
            
            if("CompositeSerializer".equals(gSampler.getCSerializerType())){
            	gSampler.setCompositeCSerializerTypes(COMPOSITE_CSERIALIZERS_LIST.getListModelAsArray());
            }
            
            if("CompositeSerializer".equals(gSampler.getVSerializerType())){
            	gSampler.setCompositeVSerializerTypes(COMPOSITE_VSERIALIZERS_LIST.getListModelAsArray());
            }
                              
        }
    }

    public void initFields()
    {     	
    	INDEX_COLUMN_NAME.setText("${__Random(1,1000)}");
    	INDEX_COLUMN_VALUE.setText("${__Random(1,1000)}");

    	START_COLUMN_NAME.setText("${__Random(1,1000)}");
        END_COLUMN_NAME.setText("${__Random(1,1000)}");
        COUNT.setText("100");
        IS_REVERSE.setSelected(false);

        KSERIALIZER.setSelectedItem("Key Serializer");
        CSERIALIZER.setSelectedItem("Column Serializer");
        VSERIALIZER.setSelectedItem("Value Serializer");
    }

    @Override
    public void init(JPanel mainPanel, GridBagConstraints labelConstraints, GridBagConstraints editConstraints)
    {
    	
    	int y = 3;

        addToPanel(mainPanel, labelConstraints, 0, y, new JLabel("Index Column Name: ", JLabel.RIGHT));
        addToPanel(mainPanel, editConstraints, 1, y, INDEX_COLUMN_NAME = new JTextField());

    	addToPanel(mainPanel, labelConstraints, 0, ++y, new JLabel("Index Column Value: ", JLabel.RIGHT));
        addToPanel(mainPanel, editConstraints, 1, y, INDEX_COLUMN_VALUE = new JTextField());

        addToPanel(mainPanel, labelConstraints, 0, ++y, new JLabel("Start Column Name: ", JLabel.RIGHT));
        addToPanel(mainPanel, editConstraints, 1, y, START_COLUMN_NAME = new JTextField());
        
        addToPanel(mainPanel, labelConstraints, 0, ++y, new JLabel("End Column Name: ", JLabel.RIGHT));
        addToPanel(mainPanel, editConstraints, 1, y, END_COLUMN_NAME = new JTextField());
        
        addToPanel(mainPanel, labelConstraints, 0, ++y, new JLabel("Count: ", JLabel.RIGHT));
        addToPanel(mainPanel, editConstraints, 1, y, COUNT = new JTextField());
        
        addToPanel(mainPanel, labelConstraints, 0, ++y, new JLabel("Reverse: ", JLabel.RIGHT));
        addToPanel(mainPanel, editConstraints, 1, y, IS_REVERSE = new JCheckBox());        
        
        addToPanel(mainPanel, labelConstraints, 0, ++y, new JLabel("Key Serializer: ", JLabel.RIGHT));
        addToPanel(mainPanel, editConstraints, 1, y, KSERIALIZER = new JComboBox(EnhancedAbstractSampler.getSerializerNames().toArray()));
        
        addToPanel(mainPanel, editConstraints, 1, ++y, COMPOSITE_KSERIALIZERS_LIST = new CompositeSerializerList());
        KSERIALIZER.addActionListener(COMPOSITE_KSERIALIZERS_LIST);

        addToPanel(mainPanel, labelConstraints, 0, ++y, new JLabel("Column Serializer: ", JLabel.RIGHT));
        addToPanel(mainPanel, editConstraints, 1, y, CSERIALIZER = new JComboBox(EnhancedAbstractSampler.getSerializerNames().toArray()));
        
        addToPanel(mainPanel, editConstraints, 1, ++y, COMPOSITE_CSERIALIZERS_LIST = new CompositeSerializerList());
        CSERIALIZER.addActionListener(COMPOSITE_CSERIALIZERS_LIST);
        
        addToPanel(mainPanel, labelConstraints, 0, ++y, new JLabel("Value Serializer: ", JLabel.RIGHT));
        addToPanel(mainPanel, editConstraints, 1, y, VSERIALIZER = new JComboBox(EnhancedAbstractSampler.getSerializerNames().toArray()));
        
        addToPanel(mainPanel, editConstraints, 1, ++y, COMPOSITE_VSERIALIZERS_LIST = new CompositeSerializerList());
        VSERIALIZER.addActionListener(COMPOSITE_VSERIALIZERS_LIST);
        
    }


	@Override
	public String getLabel() {	
		return LABEL;
	}
}
